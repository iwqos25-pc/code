## Prototype

The architecture follows master-worker style, like many state-of-art distributed file storage such as HDFS and Ceph. Four major components are client, coordinator, proxy and datanode. 

### Environment Configuration

- gcc-9.4.0

- Required packages

  * grpc v1.50

  * asio 1.24.0

  * jerasure

- we call them third_party libraries, and the source codes are provided in the `third_party/` directory.

- Before installing these packages, you should install the dependencies of grpc.

  - ```
    sudo apt install -y build-essential autoconf libtool pkg-config
    ```

- Run the following command to install these packages

  - ```
    sh install_third_party.sh
    ```

### Compile and Run

- Compile

```
cd project
sh compile.sh
```

- Run

```
sh run_proxy_datanode.sh
cd project/cmake/build
./run_coordinator
./run_client 6 10 0.0.0.0 HPC Vertical false 32 1024 2 3 1 2 1 false
```

- The parameter meaning of `run_client`

```
# for LRC
./run_client rack_num node_num_in_rack coordinator_ip encode_type multistripes_placement_type partial_decoding stripe_num value_length x k l g approach
# for HPC
./run_client rack_num node_num_in_rack coordinator_ip encode_type multistripes_placement_type partial_decoding stripe_num value_length x k1 m1 k2 m2 approach
```

#### Tips. 

- `partial_decoding` denotes if apply `encode-and-transfer`.
- `encode_type` denotes the encoding type of a single stripe, such as `RS`,  `Azure_LRC`, etc. Now support `RS`，  `Azure_LRC` and `HPC(Hierarchical Product Codes)`.
- Default  data placement type of a single stripe:  `Optimal`.
- `multistripes_placement_type` denotes the data placement type of multiple stripes, such as `Ran`, `DIS`, `AGG` and `OPT` for `LRC`, `Vertical` and `Horizontal` for `HPC`. 
-  `x` denotes the number of stripes to merge into a large-size stripe while merging. 
- `value_length` is the object size of each object to form a stripe initially, with the unit of `KiB`.

### Other

#### Directory

- directory `doc/`  is the introduction of system implementation.
- directory `project/` is the system implementation.
- create directory `data/` to store the original test data object for the client to upload.
- create directory `client_get/` to store the data object getting from proxy for the client.
- create directory `storage/` to store the data blocks for data nodes.
- create directory `run_cluster_sh/` to store the running shell for each cluster.

#### Tools

- use `small_tools/generator_file.py` to generate files with random string of specified length.
- use `small_tools/generator_sh.py` to generate configuration file and running shell for proxy and data node.

