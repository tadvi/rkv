/*
    Copyright (c) 2014-2015 Rkv Authors.
    Released under MIT license.

    This is based on https://code.google.com/p/gocask/ initial work by andrebq.

    Implementation of the bitcask key-value store from Riak.
    Paper available at: http://downloads.basho.com/papers/bitcask-intro.pdf
    The key-value pairs is stored in a log file, which only writes to the append, so a write never include a disk seek.
    Each record is stored in the following format:

   	|-------------------------------------------------------------------------------------------------------------------|
	|crc (int32) | tstamp (int32) | key length (int32) | value length (int32) | key data ([]byte) | value data ([]byte) |
	|-------------------------------------------------------------------------------------------------------------------|

    We use mostly same format but diverge in few aspects:
    1. If []byte value contains no data and is empty array, then it is deleted key, no data.
    2. tstamp contains days or 0. If tstamp is not 0 and it is less than todays day, key record has expired.
        Use PutDays to take advantage of automatic record expiration.
    3. Compact and AutoCompact reads database and compacts it.
    4. Internally structs stored as JSON.

    This is decent format for databases up to 50K records.
*/
package rkv
