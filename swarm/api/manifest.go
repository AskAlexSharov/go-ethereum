// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package api

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/swarm/storage/feed"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/swarm/log"
	"github.com/ethereum/go-ethereum/swarm/storage"
)

const (
	ManifestType    = "application/bzz-manifest+json"
	FeedContentType = "application/bzz-feed"

	manifestSizeLimit = 5 * 1024 * 1024
)

// Manifest represents a swarm manifest
type Manifest struct {
	Entries      []ManifestEntry `json:"entries,omitempty"`
	IsDirectory  bool            `json:"isDirectory,omitempty"`
	DefaultEntry string          `json:"defaultEntry,omitempty"`
}

// ManifestEntry represents an entry in a swarm manifest
type ManifestEntry struct {
	Hash        string       `json:"hash,omitempty"`
	Path        string       `json:"path,omitempty"`
	ContentType string       `json:"contentType,omitempty"`
	Mode        int64        `json:"mode,omitempty"`
	Size        int64        `json:"size,omitempty"`
	ModTime     time.Time    `json:"mod_time,omitempty"`
	Status      int          `json:"status,omitempty"`
	Access      *AccessEntry `json:"access,omitempty"`
	Feed        *feed.Feed   `json:"feed,omitempty"`
}

// ManifestList represents the result of listing files in a manifest
type ManifestList struct {
	CommonPrefixes []string         `json:"common_prefixes,omitempty"`
	Entries        []*ManifestEntry `json:"entries,omitempty"`
}

// NewManifest creates and stores a new, empty manifest
func (a *API) NewManifest(ctx context.Context, toEncrypt bool) (storage.Address, error) {
	var manifest Manifest
	data, err := json.Marshal(&manifest)
	if err != nil {
		return nil, err
	}
	addr, wait, err := a.Store(ctx, bytes.NewReader(data), int64(len(data)), toEncrypt)
	if err != nil {
		return nil, err
	}
	err = wait(ctx)
	return addr, err
}

// Manifest hack for supporting Swarm feeds from the bzz: scheme
// see swarm/api/api.go:API.Get() for more information
func (a *API) NewFeedManifest(ctx context.Context, feed *feed.Feed) (storage.Address, error) {
	var manifest Manifest
	entry := ManifestEntry{
		Feed:        feed,
		ContentType: FeedContentType,
	}
	manifest.Entries = append(manifest.Entries, entry)
	data, err := json.Marshal(&manifest)
	if err != nil {
		return nil, err
	}
	addr, wait, err := a.Store(ctx, bytes.NewReader(data), int64(len(data)), false)
	if err != nil {
		return nil, err
	}
	err = wait(ctx)
	return addr, err
}

// ManifestWriter is used to add and remove entries from an underlying manifest
type ManifestWriter struct {
	api   *API
	trie  *manifestTrie
	quitC chan bool
}

func (a *API) NewManifestWriter(ctx context.Context, addr storage.Address, quitC chan bool) (*ManifestWriter, error) {
	trie, err := loadManifest(ctx, a.fileStore, addr, quitC, NOOPDecrypt)
	if err != nil {
		return nil, fmt.Errorf("error loading manifest %s: %s", addr, err)
	}
	return &ManifestWriter{a, trie, quitC}, nil
}

// AddEntry stores the given data and adds the resulting address to the manifest
func (m *ManifestWriter) AddEntry(ctx context.Context, data io.Reader, e *ManifestEntry) (addr storage.Address, err error) {
	entry := newManifestTrieEntry(e, nil)
	if data != nil {
		var wait func(context.Context) error
		log.Trace("storing file from incoming request", "size", e.Size)
		addr, wait, err = m.api.Store(ctx, data, e.Size, m.trie.encrypted)
		if err != nil {
			return nil, err
		}
		err = wait(ctx)
		if err != nil {
			return nil, err
		}
		entry.Hash = addr.Hex()
	}
	if entry.Hash == "" {
		return addr, errors.New("missing entry hash")
	}
	m.trie.addEntry(entry, m.quitC)
	return addr, nil
}

// SetDefaultEntry sets a string as the current trie's default entry path.
func (m *ManifestWriter) SetDefaultEntry(path string) error {
	m.trie.defaultEntry = path
	return nil
}

// RemoveEntry removes the given path from the manifest
func (m *ManifestWriter) RemoveEntry(path string) error {
	m.trie.deleteEntry(path, m.quitC)
	return nil
}

// Store stores the manifest, returning the resulting storage address
func (m *ManifestWriter) Store() (storage.Address, error) {
	return m.trie.ref, m.trie.recalcAndStore()
}

// ManifestWalker is used to recursively walk the entries in the manifest and
// all of its submanifests
type ManifestWalker struct {
	api   *API
	trie  *manifestTrie
	quitC chan bool
}

func (a *API) NewManifestWalker(ctx context.Context, addr storage.Address, decrypt DecryptFunc, quitC chan bool) (*ManifestWalker, error) {
	trie, err := loadManifest(ctx, a.fileStore, addr, quitC, decrypt)
	if err != nil {
		return nil, fmt.Errorf("error loading manifest %s: %s", addr, err)
	}
	return &ManifestWalker{a, trie, quitC}, nil
}

// ErrSkipManifest is used as a return value from WalkFn to indicate that the
// manifest should be skipped
var ErrSkipManifest = errors.New("skip this manifest")

// WalkFn is the type of function called for each entry visited by a recursive
// manifest walk
type WalkFn func(entry *ManifestEntry) error

// Walk recursively walks the manifest calling walkFn for each entry in the
// manifest, including submanifests
func (m *ManifestWalker) Walk(walkFn WalkFn) error {
	return m.walk(m.trie, "", walkFn)
}

func (m *ManifestWalker) walk(trie *manifestTrie, prefix string, walkFn WalkFn) error {
	for _, entry := range &trie.entries {
		if entry == nil {
			continue
		}
		entry.Path = prefix + entry.Path
		err := walkFn(&entry.ManifestEntry)
		if err != nil {
			if entry.ContentType == ManifestType && err == ErrSkipManifest {
				continue
			}
			return err
		}
		if entry.ContentType != ManifestType {
			continue
		}
		if err := trie.loadSubTrie(entry, nil); err != nil {
			return err
		}
		if err := m.walk(entry.subtrie, entry.Path, walkFn); err != nil {
			return err
		}
	}
	return nil
}

type manifestTrie struct {
	fileStore    *storage.FileStore
	entries      [257]*manifestTrieEntry // indexed by first character of basePath, entries[256] is the empty basePath entry
	ref          storage.Address         // if ref != nil, it is stored
	encrypted    bool
	defaultEntry string
	decrypt      DecryptFunc
}

func newManifestTrieEntry(entry *ManifestEntry, subtrie *manifestTrie) *manifestTrieEntry {
	return &manifestTrieEntry{
		ManifestEntry: *entry,
		subtrie:       subtrie,
	}
}

type manifestTrieEntry struct {
	ManifestEntry

	subtrie *manifestTrie
}

func loadManifest(ctx context.Context, fileStore *storage.FileStore, addr storage.Address, quitC chan bool, decrypt DecryptFunc) (trie *manifestTrie, err error) { // non-recursive, subtrees are downloaded on-demand
	log.Trace("manifest lookup", "addr", addr)
	// retrieve manifest via FileStore
	manifestReader, isEncrypted := fileStore.Retrieve(ctx, addr)
	log.Trace("reader retrieved", "addr", addr)
	return readManifest(manifestReader, addr, fileStore, isEncrypted, quitC, decrypt)
}

func readManifest(mr storage.LazySectionReader, addr storage.Address, fileStore *storage.FileStore, isEncrypted bool, quitC chan bool, decrypt DecryptFunc) (trie *manifestTrie, err error) { // non-recursive, subtrees are downloaded on-demand

	// TODO check size for oversized manifests
	size, err := mr.Size(mr.Context(), quitC)
	if err != nil { // size == 0
		// can't determine size means we don't have the root chunk
		log.Trace("manifest not found", "addr", addr)
		err = fmt.Errorf("Manifest not Found")
		return
	}
	if size > manifestSizeLimit {
		log.Warn("manifest exceeds size limit", "addr", addr, "size", size, "limit", manifestSizeLimit)
		err = fmt.Errorf("Manifest size of %v bytes exceeds the %v byte limit", size, manifestSizeLimit)
		return
	}
	manifestData := make([]byte, size)
	read, err := mr.Read(manifestData)
	if int64(read) < size {
		log.Trace("manifest not found", "addr", addr)
		if err == nil {
			err = fmt.Errorf("Manifest retrieval cut short: read %v, expect %v", read, size)
		}
		return
	}

	log.Debug("manifest retrieved", "addr", addr)

	mm := &Manifest{}
	err = json.Unmarshal(manifestData, mm)
	if err != nil {
		err = fmt.Errorf("Manifest %v is malformed: %v", addr.Log(), err)
		log.Trace("malformed manifest", "addr", addr)
		return
	}

	log.Trace("manifest entries", "addr", addr, "len", len(mm.Entries))

	trie = &manifestTrie{
		fileStore:    fileStore,
		encrypted:    isEncrypted,
		decrypt:      decrypt,
		defaultEntry: mm.DefaultEntry,
		//		isDirectory:  mm.IsDirectory,
	}
	for _, entry := range mm.Entries {
		me := ManifestEntry{
			Hash:        entry.Hash,
			Path:        entry.Path,
			ContentType: entry.ContentType,
			Mode:        entry.Mode,
			Size:        entry.Size,
			ModTime:     entry.ModTime,
			Status:      entry.Status,
			Access:      entry.Access,
		}
		mte := &manifestTrieEntry{ManifestEntry: me}

		err = trie.addEntry(mte, quitC)
		if err != nil {
			return
		}
	}
	return
}

func (mt *manifestTrie) addEntry(entry *manifestTrieEntry, quitC chan bool) error {
	mt.ref = nil // trie modified, hash needs to be re-calculated on demand

	if entry.ManifestEntry.Access != nil {
		if mt.decrypt == nil {
			return errors.New("dont have decryptor")
		}

		err := mt.decrypt(&entry.ManifestEntry)
		if err != nil {
			return err
		}
	}

	b := entry.Path[0]
	oldentry := mt.entries[b]
	if (oldentry == nil) || (oldentry.Path == entry.Path && oldentry.ContentType != ManifestType) {
		mt.entries[b] = entry
		return nil
	}

	cpl := 0
	for (len(entry.Path) > cpl) &&
		(len(oldentry.Path) > cpl) &&
		(entry.Path[cpl] == oldentry.Path[cpl]) {
		cpl++
	}

	if (oldentry.ContentType == ManifestType) && (cpl == len(oldentry.Path)) {
		if mt.loadSubTrie(oldentry, quitC) != nil {
			return nil
		}
		entry.Path = entry.Path[cpl:]
		oldentry.subtrie.addEntry(entry, quitC)
		oldentry.Hash = ""
		return nil
	}

	commonPrefix := entry.Path[:cpl]

	subtrie := &manifestTrie{
		fileStore: mt.fileStore,
		encrypted: mt.encrypted,
	}
	entry.Path = entry.Path[cpl:]
	oldentry.Path = oldentry.Path[cpl:]
	subtrie.addEntry(entry, quitC)
	subtrie.addEntry(oldentry, quitC)

	mt.entries[b] = newManifestTrieEntry(&ManifestEntry{
		Path:        commonPrefix,
		ContentType: ManifestType,
	}, subtrie)
	return nil
}

func (mt *manifestTrie) getCountLast() (cnt int, entry *manifestTrieEntry) {
	for _, e := range &mt.entries {
		if e != nil {
			cnt++
			entry = e
		}
	}
	return
}

func (mt *manifestTrie) deleteEntry(path string, quitC chan bool) {
	mt.ref = nil // trie modified, hash needs to be re-calculated on demand

	b := path[0]
	entry := mt.entries[b]
	if entry == nil {
		return
	}
	if entry.Path == path {
		mt.entries[b] = nil
		return
	}

	epl := len(entry.Path)
	if (entry.ContentType == ManifestType) && (len(path) >= epl) && (path[:epl] == entry.Path) {
		if mt.loadSubTrie(entry, quitC) != nil {
			return
		}
		entry.subtrie.deleteEntry(path[epl:], quitC)
		entry.Hash = ""
		// remove subtree if it has less than 2 elements
		cnt, lastentry := entry.subtrie.getCountLast()
		if cnt < 2 {
			if lastentry != nil {
				lastentry.Path = entry.Path + lastentry.Path
			}
			mt.entries[b] = lastentry
		}
	}
}

func (mt *manifestTrie) recalcAndStore() error {
	if mt.ref != nil {
		return nil
	}

	list := &Manifest{}
	list.DefaultEntry = mt.defaultEntry
	for _, entry := range &mt.entries {
		if entry != nil {
			if entry.Hash == "" { // TODO: paralellize
				err := entry.subtrie.recalcAndStore()
				if err != nil {
					return err
				}
				entry.Hash = entry.subtrie.ref.Hex()
			}
			list.Entries = append(list.Entries, entry.ManifestEntry)
		}
	}

	manifest, err := json.Marshal(list)
	if err != nil {
		return err
	}

	sr := bytes.NewReader(manifest)
	ctx := context.TODO()
	addr, wait, err2 := mt.fileStore.Store(ctx, sr, int64(len(manifest)), mt.encrypted)
	if err2 != nil {
		return err2
	}
	err2 = wait(ctx)
	mt.ref = addr
	return err2
}

func (mt *manifestTrie) loadSubTrie(entry *manifestTrieEntry, quitC chan bool) (err error) {
	if entry.ManifestEntry.Access != nil {
		if mt.decrypt == nil {
			return errors.New("dont have decryptor")
		}

		err := mt.decrypt(&entry.ManifestEntry)
		if err != nil {
			return err
		}
	}

	if entry.subtrie == nil {
		hash := common.Hex2Bytes(entry.Hash)
		entry.subtrie, err = loadManifest(context.TODO(), mt.fileStore, hash, quitC, mt.decrypt)
		entry.Hash = "" // might not match, should be recalculated
	}
	return
}

func (mt *manifestTrie) listWithPrefixInt(prefix, rp string, quitC chan bool, cb func(entry *manifestTrieEntry, suffix string)) error {
	plen := len(prefix)
	var start, stop int
	if plen == 0 {
		start = 0
		stop = 256
	} else {
		start = int(prefix[0])
		stop = start
	}

	for i := start; i <= stop; i++ {
		select {
		case <-quitC:
			return fmt.Errorf("aborted")
		default:
		}
		entry := mt.entries[i]
		if entry != nil {
			epl := len(entry.Path)
			if entry.ContentType == ManifestType {
				l := plen
				if epl < l {
					l = epl
				}
				if prefix[:l] == entry.Path[:l] {
					err := mt.loadSubTrie(entry, quitC)
					if err != nil {
						return err
					}
					err = entry.subtrie.listWithPrefixInt(prefix[l:], rp+entry.Path[l:], quitC, cb)
					if err != nil {
						return err
					}
				}
			} else {
				if (epl >= plen) && (prefix == entry.Path[:plen]) {
					cb(entry, rp+entry.Path[plen:])
				}
			}
		}
	}
	return nil
}

func (mt *manifestTrie) listWithPrefix(prefix string, quitC chan bool, cb func(entry *manifestTrieEntry, suffix string)) (err error) {
	return mt.listWithPrefixInt(prefix, "", quitC, cb)
}

func (mt *manifestTrie) findPrefixOf(path string, quitC chan bool) (entry *manifestTrieEntry, pos int) {
	log.Trace(fmt.Sprintf("findPrefixOf(%s)", path))

	//see if first char is in manifest entries
	b := path[0]
	entry = mt.entries[b]
	epl := len(entry.Path)
	log.Trace(fmt.Sprintf("path = %v  entry.Path = %v  epl = %v", path, entry.Path, epl))
	if len(path) <= epl {
		if entry.Path[:len(path)] == path {
			if entry.ContentType == ManifestType {
				err := mt.loadSubTrie(entry, quitC)
				if err == nil && entry.subtrie != nil {
					subentries := entry.subtrie.entries
					for i := 0; i < len(subentries); i++ {
						sub := subentries[i]
						if sub != nil && sub.Path == "" {
							return sub, len(path)
						}
					}
				}
				entry.Status = http.StatusMultipleChoices
			}
			pos = len(path)
			return
		}
		return nil, 0
	}
	if path[:epl] == entry.Path {
		log.Trace(fmt.Sprintf("entry.ContentType = %v", entry.ContentType))
		//the subentry is a manifest, load subtrie
		if entry.ContentType == ManifestType && (strings.Contains(entry.Path, path) || strings.Contains(path, entry.Path)) {
			err := mt.loadSubTrie(entry, quitC)
			if err != nil {
				return nil, 0
			}
			sub, pos := entry.subtrie.findPrefixOf(path[epl:], quitC)
			if sub != nil {
				entry = sub
				pos += epl
				return sub, pos
			} else if path == entry.Path {
				entry.Status = http.StatusMultipleChoices
			}

		} else {
			//entry is not a manifest, return it
			if path != entry.Path {
				return nil, 0
			}
			pos = epl
		}
	}
	return nil, 0
}

// file system manifest always contains regularized paths
// no leading or trailing slashes, only single slashes inside
func RegularSlashes(path string) (res string) {
	for i := 0; i < len(path); i++ {
		if (path[i] != '/') || ((i > 0) && (path[i-1] != '/')) {
			res = res + path[i:i+1]
		}
	}
	if (len(res) > 0) && (res[len(res)-1] == '/') {
		res = res[:len(res)-1]
	}
	return
}

func (mt *manifestTrie) getEntry(spath string) (entry *manifestTrieEntry, fullpath string) {
	log.Debug("api.manifest.getEntry", "spath", spath)
	path := RegularSlashes(spath)
	var pos int
	quitC := make(chan bool)
	entry, pos = mt.findPrefixOf(path, quitC)
	return entry, path[:pos]
}

func (a *API) UpdateManifest(ctx context.Context, addr storage.Address, update func(mw *ManifestWriter) error) (storage.Address, error) {
	apiManifestUpdateCount.Inc(1)
	mw, err := a.NewManifestWriter(ctx, addr, nil)
	if err != nil {
		apiManifestUpdateFail.Inc(1)
		return nil, err
	}

	if err := update(mw); err != nil {
		apiManifestUpdateFail.Inc(1)
		return nil, err
	}

	addr, err = mw.Store()
	if err != nil {
		apiManifestUpdateFail.Inc(1)
		return nil, err
	}
	log.Debug(fmt.Sprintf("generated manifest %s", addr))
	return addr, nil
}

// Modify loads manifest and checks the content hash before recalculating and storing the manifest.
func (a *API) Modify(ctx context.Context, addr storage.Address, path, contentHash, contentType string) (storage.Address, error) {
	apiModifyCount.Inc(1)
	quitC := make(chan bool)
	trie, err := loadManifest(ctx, a.fileStore, addr, quitC, NOOPDecrypt)
	if err != nil {
		apiModifyFail.Inc(1)
		return nil, err
	}
	if contentHash != "" {
		entry := newManifestTrieEntry(&ManifestEntry{
			Path:        path,
			ContentType: contentType,
		}, nil)
		entry.Hash = contentHash
		trie.addEntry(entry, quitC)
	} else {
		trie.deleteEntry(path, quitC)
	}

	if err := trie.recalcAndStore(); err != nil {
		apiModifyFail.Inc(1)
		return nil, err
	}
	return trie.ref, nil
}

// AddFile creates a new manifest entry, adds it to swarm, then adds a file to swarm.
func (a *API) AddFile(ctx context.Context, mhash, path, fname string, content []byte, nameresolver bool) (storage.Address, string, error) {
	apiAddFileCount.Inc(1)

	mkey, err := a.Resolve(ctx, mhash)
	if err != nil {
		apiAddFileFail.Inc(1)
		return nil, "", err
	}

	// trim the root dir we added
	if path[:1] == "/" {
		path = path[1:]
	}

	entry := &ManifestEntry{
		Path:        filepath.Join(path, fname),
		ContentType: mime.TypeByExtension(filepath.Ext(fname)),
		Mode:        0700,
		Size:        int64(len(content)),
		ModTime:     time.Now(),
	}

	mw, err := a.NewManifestWriter(ctx, mkey, nil)
	if err != nil {
		apiAddFileFail.Inc(1)
		return nil, "", err
	}

	fkey, err := mw.AddEntry(ctx, bytes.NewReader(content), entry)
	if err != nil {
		apiAddFileFail.Inc(1)
		return nil, "", err
	}

	newMkey, err := mw.Store()
	if err != nil {
		apiAddFileFail.Inc(1)
		return nil, "", err

	}

	return fkey, newMkey.String(), nil
}

func (a *API) UploadTar(ctx context.Context, bodyReader io.ReadCloser, manifestPath, defaultPath string, mw *ManifestWriter) (storage.Address, error) {
	apiUploadTarCount.Inc(1)
	var contentKey storage.Address
	tr := tar.NewReader(bodyReader)
	defer bodyReader.Close()
	var defaultPathFound bool
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			apiUploadTarFail.Inc(1)
			return nil, fmt.Errorf("error reading tar stream: %s", err)
		}

		// only store regular files
		if !hdr.FileInfo().Mode().IsRegular() {
			continue
		}

		// add the entry under the path from the request
		manifestPath := path.Join(manifestPath, hdr.Name)
		contentType := hdr.Xattrs["user.swarm.content-type"]
		if contentType == "" {
			contentType = mime.TypeByExtension(filepath.Ext(hdr.Name))
		}
		log.Error("putting manifest path file", "mpath", manifestPath, "size", hdr.Size)
		entry := &ManifestEntry{
			Path:        manifestPath,
			ContentType: contentType,
			Mode:        hdr.Mode,
			Size:        hdr.Size,
			ModTime:     hdr.ModTime,
		}

		contentKey, err = mw.AddEntry(ctx, tr, entry)
		if err != nil {
			apiUploadTarFail.Inc(1)
			return nil, fmt.Errorf("Error adding manifest entry from tar stream: %s", err)
		}
		log.Error("got content key", "key", contentKey)

		if hdr.Name == defaultPath {
			log.Info("writing defualt entry")
			err := mw.SetDefaultEntry(defaultPath)
			if err != nil {
				apiUploadTarFail.Inc(1)
				return nil, fmt.Errorf("Error setting default manifest entry from tar stream: %s", err)
			}
			defaultPathFound = true
		}
	}
	if defaultPath != "" && !defaultPathFound {
		return contentKey, fmt.Errorf("default path %q not found", defaultPath)
	}
	return contentKey, nil
}

// RemoveFile removes a file entry in a manifest.
func (a *API) RemoveFile(ctx context.Context, mhash string, path string, fname string, nameresolver bool) (string, error) {
	apiRmFileCount.Inc(1)

	uri, err := Parse("bzz:/" + mhash)
	if err != nil {
		apiRmFileFail.Inc(1)
		return "", err
	}
	mkey, err := a.ResolveURI(ctx, uri, EMPTY_CREDENTIALS)
	if err != nil {
		apiRmFileFail.Inc(1)
		return "", err
	}

	// trim the root dir we added
	if path[:1] == "/" {
		path = path[1:]
	}

	mw, err := a.NewManifestWriter(ctx, mkey, nil)
	if err != nil {
		apiRmFileFail.Inc(1)
		return "", err
	}

	err = mw.RemoveEntry(filepath.Join(path, fname))
	if err != nil {
		apiRmFileFail.Inc(1)
		return "", err
	}

	newMkey, err := mw.Store()
	if err != nil {
		apiRmFileFail.Inc(1)
		return "", err

	}

	return newMkey.String(), nil
}

// AppendFile removes old manifest, appends file entry to new manifest and adds it to Swarm.
func (a *API) AppendFile(ctx context.Context, mhash, path, fname string, existingSize int64, content []byte, oldAddr storage.Address, offset int64, addSize int64, nameresolver bool) (storage.Address, string, error) {
	apiAppendFileCount.Inc(1)

	buffSize := offset + addSize
	if buffSize < existingSize {
		buffSize = existingSize
	}

	buf := make([]byte, buffSize)

	oldReader, _ := a.Retrieve(ctx, oldAddr)
	io.ReadAtLeast(oldReader, buf, int(offset))

	newReader := bytes.NewReader(content)
	io.ReadAtLeast(newReader, buf[offset:], int(addSize))

	if buffSize < existingSize {
		io.ReadAtLeast(oldReader, buf[addSize:], int(buffSize))
	}

	combinedReader := bytes.NewReader(buf)
	totalSize := int64(len(buf))

	// TODO(jmozah): to append using pyramid chunker when it is ready
	//oldReader := a.Retrieve(oldKey)
	//newReader := bytes.NewReader(content)
	//combinedReader := io.MultiReader(oldReader, newReader)

	uri, err := Parse("bzz:/" + mhash)
	if err != nil {
		apiAppendFileFail.Inc(1)
		return nil, "", err
	}
	mkey, err := a.ResolveURI(ctx, uri, EMPTY_CREDENTIALS)
	if err != nil {
		apiAppendFileFail.Inc(1)
		return nil, "", err
	}

	// trim the root dir we added
	if path[:1] == "/" {
		path = path[1:]
	}

	mw, err := a.NewManifestWriter(ctx, mkey, nil)
	if err != nil {
		apiAppendFileFail.Inc(1)
		return nil, "", err
	}

	err = mw.RemoveEntry(filepath.Join(path, fname))
	if err != nil {
		apiAppendFileFail.Inc(1)
		return nil, "", err
	}

	entry := &ManifestEntry{
		Path:        filepath.Join(path, fname),
		ContentType: mime.TypeByExtension(filepath.Ext(fname)),
		Mode:        0700,
		Size:        totalSize,
		ModTime:     time.Now(),
	}

	fkey, err := mw.AddEntry(ctx, io.Reader(combinedReader), entry)
	if err != nil {
		apiAppendFileFail.Inc(1)
		return nil, "", err
	}

	newMkey, err := mw.Store()
	if err != nil {
		apiAppendFileFail.Inc(1)
		return nil, "", err

	}

	return fkey, newMkey.String(), nil
}
