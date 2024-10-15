#ifndef RS_H
#define RS_H
#include "jerasure.h"
#include "reed_sol.h"
#include "cauchy.h"
#include "meta_definition.h"
#include <stdio.h>

namespace ECProject
{
    bool encode_RS(int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize);
    bool decode_RS(int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, int *erasures, int failed_num);
    bool encode_partial_blocks_with_data_blocks_RS(int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, std::shared_ptr<std::vector<int>> data_idx_ptrs, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs);
    bool encode_partial_blocks_for_repair_RS(int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, std::shared_ptr<std::vector<int>> data_idx_ptrs, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs);
    bool decode_with_partial_blocks_RS(int k, int m, char **partial_or_parity, char **repair_ptrs, int blocksize, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs, int *erasures);
}
#endif