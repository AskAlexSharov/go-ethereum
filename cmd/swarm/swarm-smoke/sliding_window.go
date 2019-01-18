// Copyright 2018 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/swarm/testutil"
	"github.com/pborman/uuid"

	cli "gopkg.in/urfave/cli.v1"
)

const arbitraryJ = 100

type uploadResult struct {
	hash   string
	digest string
}

func slidingWindow(c *cli.Context) error {
	// test dscription:
	// 1. upload repeatedly the same file size, maintain a slice in which swarm hashes are stored, first hash at idx=0
	// 2. select a random node, start downloading the hashes, starting with the LAST one first (it should always be availble), till the FIRST hash
	// 3. when

	defer func(now time.Time) {
		totalTime := time.Since(now)

		log.Info("total time", "time", totalTime)
		metrics.GetOrRegisterCounter("sliding-window.total-time", nil).Inc(int64(totalTime))
	}(time.Now())

	generateEndpoints(scheme, cluster, appName, from, to)
	const storeSize = 5000
	const nodes = 20                                                   //todo this should be a param
	const networkSizeFactor = 1 / nodes                                //we should aspire that this should be 1.0
	deploymentStoreSize := storeSize * 1000                            //bytes. todo move this to be a param, when testing we should test that this value passes. theoretically our J should be store capacity * nodes
	hashes := []uploadResult{}                                         //swarm hashes of the uploads
	filesize := deploymentStoreSize / 10                               //each file to upload
	networkCapacity := deploymentStoreSize * nodes * networkSizeFactor //theoretically this should be very near to nodes * storeSize
	const iterationTimeout = 30 * time.Second
	log.Info("sliding window test started", "store size", storeSize, "sizeFactor", networkSizeFactor, "nodes", nodes, "filesize", filesize, "network capacity", networkCapacity)
	uploadedBytes := 0
	for uploadedBytes = 0; uploadedBytes <= networkCapacity; uploadedBytes += filesize {
		seed := int(time.Now().UnixNano() / 1e6)
		log.Info("uploading to "+endpoints[0]+" and syncing", "seed", seed)

		randomBytes := testutil.RandomBytes(seed, filesize)

		t1 := time.Now()
		hash, err := upload(&randomBytes, endpoints[0])
		if err != nil {
			log.Error(err.Error())
			return err
		}
		metrics.GetOrRegisterCounter("sliding-window.upload-time", nil).Inc(int64(time.Since(t1)))

		fhash, err := digest(bytes.NewReader(randomBytes))
		if err != nil {
			log.Error(err.Error())
			return err
		}

		log.Info("uploaded successfully", "hash", hash, "digest", fmt.Sprintf("%x", fhash))
		hashes = append(hashes, uploadResult{hash: hash, digest: fhash})
	}
	time.Sleep(time.Duration(syncDelay) * time.Second)

	networkDepth := 0
	timedOut := false
	//now start downloading, last is first.
	for i := len(hashes) - 1; i >= 0; i-- {
		wg := sync.WaitGroup{}
		done := time.After(iterationTimeout)
		if single {
			rand.Seed(time.Now().UTC().UnixNano())
			randIndex := 1 + rand.Intn(len(endpoints)-1)
			ruid := uuid.New()[:8]
			wg.Add(1)
			go func(endpoint string, ruid string) {
				defer wg.Done()
				for {
					select {
					case <-done:
						break
					default:
					}

					start := time.Now()
					err := fetch(hashes[i].hash, endpoint, hashes[i].digest, ruid)
					fetchTime := time.Since(start)
					if err != nil {
						continue
					}

					metrics.GetOrRegisterMeter("sliding-window.single.fetch-time", nil).Mark(int64(fetchTime))
					return
				}
			}(endpoints[randIndex], ruid)
		} else {
			for _, endpoint := range endpoints {
				ruid := uuid.New()[:8]
				wg.Add(1)
				go func(endpoint string, ruid string) {
					defer wg.Done()
					for {
						select {
						case <-done:
							break
						default:
						}

						start := time.Now()
						err := fetch(hashes[i].hash, endpoint, hashes[i].digest, ruid)
						fetchTime := time.Since(start)
						if err != nil {
							continue
						}

						metrics.GetOrRegisterMeter("sliding-window.each.fetch-time", nil).Mark(int64(fetchTime))
						return
					}
				}(endpoint, ruid)
			}
		}

		wg.Wait()
		select {
		case <-done:
			networkDepth = len(hashes) - i
			timedOut = true
			break
		default:
		}
	}

	log.Info("sliding window test finished", "timed out?", timedOut, "networkDepth", networkDepth, "networkDepthBytes", networkDepth*filesize, "uploadedBytes", uploadedBytes)

	return nil
}
