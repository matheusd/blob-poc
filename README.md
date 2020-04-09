# Setup Infra

```
$ git clone https://github.com/ipfs/go-ipfs
$ (cd go-ipfs && make install)
$ git clone https://github.com/ipfs/ipfs-cluster
$ (cd ipfs-cluster && make install)

$ ipfs init
$ ipfs daemon # run on separate terminal

$ ipfs-cluster-service https://github.com/matheusd/blob-poc/raw/master/dcr-blob-poc.json
```

# Bootstrap Cluster

Only needed on the first setup of the first cluster peer.

```
$ ipfs-cluster-service init --consensus crdt
$ cp ~/.ipfs-cluster/service.json dcr-blob-poc.json
$ $EDITOR dcr-blob-poc.json
```

  - Remove `cluster.peername`
  - Modify `cluster.consensus.crdt.cluster_name` as appropriate
  - Modify `cluster.consensus.crdt.trusted_peers` (remove `*` add trusted peers)
  - Modify `cluster.replication_factor_min` (and `_max`) as appropriate
  - Anything else? Need to see what is absolutely needed
