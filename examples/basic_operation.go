package main

import bitcask "bitcask-go"

func main() {
	opts := bitcask.DefaultOptions
	opts.DirPath = "/tmp/bitcask-go"
	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("key1"))
	if err != nil {
		panic(err)
	}

	println(string(val))

	err = db.Delete([]byte("key1"))
	if err != nil {
		panic(err)
	}
}
