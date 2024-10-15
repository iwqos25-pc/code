#ifndef PC_H
#define PC_H
#include "jerasure.h"
#include "reed_sol.h"
#include "cauchy.h"
#include "meta_definition.h"
#include <stdio.h>

namespace ECProject
{
    bool hpc_make_matrix(int x, int k, int m, int seri_num, int *final_matrix);
	bool encode_by_row_or_col(int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize);
    bool decode_by_row_or_col(int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, int *erasures, int failed_num);
    bool encode_by_row_or_col_enlarged(int x, int seri_num, bool rv_or_ch, int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize);
    bool decode_by_row_or_col_enlarged(int x, int seri_num, bool rv_or_ch, int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, int *erasures, int failed_num);
    bool encode_PC(int k1, int m1, int k2, int m2, char **data_ptrs, char **coding_ptrs, int blocksize);
    bool encode_HPC(int x, int k1, int m1, int k2, int m2, char **data_ptrs, char **coding_ptrs, int blocksize, bool isvertical, int seri_num);
    bool encode_partial_block_with_data_blocks_HPC(int x, int seri_num, bool rv_or_ch, int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, std::shared_ptr<std::vector<int>> data_idx_ptrs, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs);
    bool encode_partial_blocks_for_repair_HPC(int x, int seri_num, bool rv_or_ch, int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, std::shared_ptr<std::vector<int>> data_idx_ptrs, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs);
    bool decode_with_partial_blocks(int x, int seri_num, bool rv_or_ch, int k, int m, char **partial_or_parity, char **repair_ptrs, int blocksize, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs, int *erasures);
    bool perform_addition_xor(char **data_ptrs, char **coding_ptrs, int blocksize, int block_num, int parity_num);
    bool decode_with_partial_blocks_HPC(int x, int seri_num, bool rv_or_ch, int k, int m, char **partial_or_parity, char **repair_ptrs, int blocksize, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs, int *erasures);
    bool encode_partial_block_with_data_blocks_PC(int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, std::shared_ptr<std::vector<int>> data_idx_ptrs, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs);
    bool encode_partial_blocks_for_repair_PC(int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, std::shared_ptr<std::vector<int>> data_idx_ptrs, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs);
    bool decode_with_partial_blocks_PC(int k, int m, char **partial_or_parity, char **repair_ptrs, int blocksize, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs, int *erasures);
}
#endif