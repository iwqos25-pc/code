#include <pc.h>

bool ECProject::hpc_make_matrix(int x, int k, int m, int seri_num, int *final_matrix)
{
	if (seri_num >= x)
	{
		std::cout << "Invalid argurments!" << std::endl;
		return false;
	}
	int *rs_matrix = reed_sol_vandermonde_coding_matrix(x * k, m, 8);
	int index = 0;
	for (int i = 0; i < m; i++)
	{
		memcpy(&final_matrix[index], &rs_matrix[i * k * x + seri_num * k], k * sizeof(int));
		index += k;
	}
	free(rs_matrix);
	return true;
}

bool ECProject::encode_by_row_or_col(int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize)
{
	int *rs_matrix = reed_sol_vandermonde_coding_matrix(k, m, 8);
	jerasure_matrix_encode(k, m, 8, rs_matrix, data_ptrs, coding_ptrs, blocksize);
	free(rs_matrix);
	return true;
}

bool ECProject::decode_by_row_or_col(int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, int *erasures, int failed_num)
{
	if (failed_num > m)
	{
		std::cout << "[Decode] Undecodable!" << std::endl;
		return false;
	}
	int *rs_matrix = reed_sol_vandermonde_coding_matrix(k, m, 8);
	int i = 0;
	i = jerasure_matrix_decode(k, m, 8, rs_matrix, failed_num, erasures, data_ptrs, coding_ptrs, blocksize);
	if (i == -1)
	{
		std::cout << "[Decode] Failed!" << std::endl;
	}
	return true;
}

bool ECProject::encode_by_row_or_col_enlarged(int x, int seri_num, bool rv_or_ch, int k, int m, char **data_ptrs, char **coding_ptrs, int block_size)
{
	if (rv_or_ch)
	{
		x = 1;
		seri_num = 0;
	}
	std::vector<int> rs_matrix(k * m, 0);
	hpc_make_matrix(x, k, m, seri_num, rs_matrix.data());
	jerasure_matrix_encode(k, m, 8, rs_matrix.data(), data_ptrs, coding_ptrs, block_size);
}

bool ECProject::decode_by_row_or_col_enlarged(int x, int seri_num, bool rv_or_ch, int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, int *erasures, int failed_num)
{
	if (failed_num > m)
	{
		std::cout << "[Decode] Undecodable!" << std::endl;
		return false;
	}
	if (rv_or_ch)
	{
		x = 1;
		seri_num = 0;
	}
	std::vector<int> rs_matrix(k * m, 0);
	hpc_make_matrix(x, k, m, seri_num, rs_matrix.data());
	int i = 0;
	i = jerasure_matrix_decode(k, m, 8, rs_matrix.data(), failed_num, erasures, data_ptrs, coding_ptrs, blocksize);
	if (i == -1)
	{
		std::cout << "[Decode] Failed!" << std::endl;
	}
	return true;
}

/*
	encode_PC:
	data_ptrs: odered by row
	D(0) ... D(k1*k2-1)
	coding_ptrs: ordered by row
	R(0) ... R(k2*m1-1) C(0) ...  C(2*k1-1) G(0) ... G(2*m1-1) ...
*/
bool ECProject::encode_PC(int k1, int m1, int k2, int m2, char **data_ptrs, char **coding_ptrs, int blocksize)
{
	// encode row parities
	for (int i = 0; i < k2; i++)
	{
		std::vector<char *> t_coding(m1);
		char **coding = (char **)t_coding.data();
		for (int j = 0; j < m1; j++)
		{
			coding[j] = coding_ptrs[i * m1 + j];
		}
		encode_by_row_or_col(k1, m1, &data_ptrs[i * k1], coding, blocksize);
	}
	// encode column parities
	for (int i = 0; i < k1 + m1; i++)
	{
		std::vector<char *> t_data(k2);
		char **data = (char **)t_data.data();
		if (i < k1)
		{
			for (int j = 0; j < k2; j++)
			{
				data[j] = data_ptrs[j * k1 + i];
			}
		}
		else
		{
			for (int j = 0; j < k2; j++)
			{
				data[j] = coding_ptrs[j * m1 + i - k1];
			}
		}
		std::vector<char *> t_coding(m2);
		char **coding = (char **)t_coding.data();
		if (i < k1)
		{
			for (int j = 0; j < m2; j++)
			{
				coding[j] = coding_ptrs[k2 * m1 + j * k1 + i];
			}
		}
		else
		{
			for (int j = 0; j < m2; j++)
			{
				coding[j] = coding_ptrs[k2 * m1 + k1 * m2 + j * m1 + i - k1];
			}
		}
		encode_by_row_or_col(k2, m2, data, coding, blocksize);
	}
	return true;
}

bool ECProject::encode_HPC(int x, int k1, int m1, int k2, int m2, char **data_ptrs, char **coding_ptrs, int blocksize, bool isvertical, int seri_num)
{
	// encode row parities
	for (int i = 0; i < k2; i++)
	{
		std::vector<char *> t_coding(m1);
		char **coding = (char **)t_coding.data();
		for (int j = 0; j < m1; j++)
		{
			coding[j] = coding_ptrs[i * m1 + j];
		}
		if (isvertical)
		{
			ECProject::encode_by_row_or_col(k1, m1, &data_ptrs[i * k1], coding, blocksize);
		}
		else
		{
			ECProject::encode_by_row_or_col_enlarged(x, seri_num, false, k1, m1, &data_ptrs[i * k1], coding, blocksize);
		}
	}
	// encode column parities
	for (int i = 0; i < k1 + m1; i++)
	{
		std::vector<char *> t_data(k2);
		char **data = (char **)t_data.data();
		if (i < k1)
		{
			for (int j = 0; j < k2; j++)
			{
				data[j] = data_ptrs[j * k1 + i];
			}
		}
		else
		{
			for (int j = 0; j < k2; j++)
			{
				data[j] = coding_ptrs[j * m1 + i - k1];
			}
		}
		std::vector<char *> t_coding(m2);
		char **coding = (char **)t_coding.data();
		if (i < k1)
		{
			for (int j = 0; j < m2; j++)
			{
				coding[j] = coding_ptrs[k2 * m1 + j * k1 + i];
			}
		}
		else
		{
			for (int j = 0; j < m2; j++)
			{
				coding[j] = coding_ptrs[k2 * m1 + k1 * m2 + j * m1 + i - k1];
			}
		}
		if (isvertical)
		{
			ECProject::encode_by_row_or_col_enlarged(x, seri_num, false, k2, m2, data, coding, blocksize);
		}
		else
		{
			ECProject::encode_by_row_or_col(k2, m2, data, coding, blocksize);
		}
	}
	return true;
}

// bool ECProject::encode_HPC(int x, int k1, int m1, int k2, int m2, char **data_ptrs, char **coding_ptrs, int blocksize, bool isvertical, int seri_num)
// {
// 	std::vector<char *> t_data(x * k1 * k2);
// 	char **data = (char **)t_data.data();
// 	std::vector<std::vector<char>> t_data_area((x - 1) * k1 * k2, std::vector<char>(blocksize));

// 	if (isvertical)
// 	{
// 		// data, add dummy data blocks
// 		int index = 0;
// 		for (int i = 0; i < x; i++)
// 		{
// 			if (i == seri_num)
// 			{
// 				for (int j = 0; j < k1 * k2; j++)
// 				{
// 					data[i * k1 * k2 + j] = data_ptrs[j];
// 				}
// 			}
// 			else
// 			{
// 				for (int j = 0; j < k1 * k2; j++)
// 				{
// 					data[i * k1 * k2 + j] = t_data_area[index++].data();
// 					memset(data[i * k1 * k2 + j], 0, blocksize);
// 				}
// 			}
// 		}

// 		// coding, add enlarged coding space
// 		std::vector<char *> t_coding(x * k2 * m1 + k1 * m2 + m1 * m2);
// 		char **coding = (char **)t_coding.data();
// 		std::vector<std::vector<char>> t_coding_area((x - 1) * k2 * m1, std::vector<char>(blocksize));
// 		int in1 = 0, in2 = 0, in3 = 0;
// 		for (int i = 0; i < x; i++)
// 		{
// 			if (i == seri_num)
// 			{
// 				for (int j = 0; j < k2 * m1; j++)
// 				{
// 					coding[in1++] = coding_ptrs[in2++];
// 				}
// 			}
// 			else
// 			{
// 				for (int j = 0; j < k2 * m1; j++)
// 				{
// 					coding[in1++] = t_coding_area[in3++].data();
// 				}
// 			}
// 		}
// 		for (int i = 0; i < (k1 + m1) * m2; i++)
// 		{
// 			coding[in1++] = coding_ptrs[in2++];
// 		}

// 		encode_PC(k1, m1, x * k2, m2, data, coding, blocksize);
// 	}
// 	else
// 	{
// 		// data, add dummy data blocks
// 		int index1 = 0, index2 = 0;
// 		for (int i = 0; i < k2; i++)
// 		{
// 			for (int j = 0; j < x; j++)
// 			{
// 				if (j == seri_num)
// 				{
// 					for (int k = 0; k < k1; k++)
// 					{
// 						data[i * x * k1 + j * k1 + k] = data_ptrs[index1++];
// 					}
// 				}
// 				else
// 				{
// 					for (int k = 0; k < k1; k++)
// 					{
// 						data[i * x * k1 + j * k1 + k] = t_data_area[index2++].data();
// 						memset(data[i * x * k1 + j * k1 + k], 0, blocksize);
// 					}
// 				}
// 			}
// 		}

// 		// coding, add enlarged coding space
// 		std::vector<char *> t_coding(x * k1 * m2 + k2 * m1 + m1 * m2);
// 		char **coding = (char **)t_coding.data();
// 		std::vector<std::vector<char>> t_coding_area((x - 1) * k1 * m2, std::vector<char>(blocksize));
// 		int in1 = 0, in2 = 0, in3 = 0;
// 		for (int i = 0; i < k2 * m1; i++)
// 		{
// 			coding[in1++] = coding_ptrs[in2++];
// 		}
// 		for (int i = 0; i < m2; i++)
// 		{
// 			for (int j = 0; j < x; j++)
// 			{
// 				if (j == seri_num)
// 				{
// 					for (int k = 0; k < k1; k++)
// 					{
// 						coding[in1++] = coding_ptrs[in2++];
// 					}
// 				}
// 				else
// 				{
// 					for (int k = 0; k < k1; k++)
// 					{
// 						coding[in1++] = t_coding_area[in3++].data();
// 					}
// 				}
// 			}
// 			for (int k = 0; k < m1; k++)
// 			{
// 				coding[in1++] = coding_ptrs[in2++];
// 			}
// 		}

// 		encode_PC(x * k1, m1, k2, m2, data, coding, blocksize);
// 	}
// 	return true;
// }

bool ECProject::encode_partial_block_with_data_blocks_HPC(int x, int seri_num, bool rv_or_ch, int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, std::shared_ptr<std::vector<int>> data_idx_ptrs, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs)
{
	/*
		> Only support encoding partial blocks with only data blocks
		> Attention! x = 1, seri_num = 0, when it's 'row and vertical' or 'column and horizontal'
	*/
	if (rv_or_ch)
	{
		x = 1;
		seri_num = 0;
	}
	std::vector<int> rs_matrix(k * m, 0);
	hpc_make_matrix(x, k, m, seri_num, rs_matrix.data());
	int parity_num = int(parity_idx_ptrs->size());
	std::vector<int> matrix(parity_num * k, 0);
	int cnt = 0;
	for (auto it = parity_idx_ptrs->begin(); it != parity_idx_ptrs->end(); it++)
	{
		int j = *it;
		memcpy(&matrix[cnt * k], &rs_matrix[j * k], k * sizeof(int));
		cnt++;
	}

	std::vector<int> new_matrix(parity_num * block_num, 1);

	int idx = 0;
	for (auto it = data_idx_ptrs->begin(); it != data_idx_ptrs->end(); it++)
	{
		for (int i = 0; i < parity_num; i++)
		{
			int j = *it;
			new_matrix[i * block_num + idx] = matrix[i * k + j];
		}
		idx++;
	}

	// for(int i = 0; i < parity_num * block_num; i++)
	// {
	// 	std::cout << new_matrix[i] << " ";
	// }
	// std::cout << std::endl;

	jerasure_matrix_encode(block_num, parity_num, 8, new_matrix.data(), data_ptrs, coding_ptrs, blocksize);

	return true;
}
bool ECProject::decode_with_partial_blocks(int x, int seri_num, bool rv_or_ch, int k, int m, char **partial_or_parity, char **repair_ptrs, int blocksize, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs, int *erasures)
{
	/*  > 'PB0-1' means the partial block 0 from the cluster 1
		> 'P0' means parity block 0
		> partial block 0 should be corresponding to parity block 0
		> The order of blocks in partial_or_parity:
			> e.g. [PB0-0], [PB1-0], [PB0-1], [PB1-1], ... , [P0], [P1]
		> block_num: the total number of partial blocks and parity blocks
		> the order of erasures should be consistent with repair_ptrs
		> Produre: xor(partial + parity) -> construct matrix and invert -> encode with the inverse matrix
		> Attention! x = 1, seri_num = 0, when it's 'row and vertical' or 'column and horizontal'
	*/
	if (rv_or_ch)
	{
		x = 1;
		seri_num = 0;
	}
	int parity_num = int(parity_idx_ptrs->size());
	if (block_num % parity_num != 0)
	{
		printf("invalid! %d mod %d != 0.\nThe number of partial blocks is not matched with the number of parity blocks\n", block_num, parity_num);
		return false;
	}
	int num_of_block_each_parity = block_num / parity_num;

	std::vector<char *> t_data(block_num);
	char **data = (char **)t_data.data();
	int cnt = 0;
	for (int i = 0; i < parity_num; i++)
	{
		for (int j = 0; j < num_of_block_each_parity; j++)
		{
			data[cnt++] = partial_or_parity[j * parity_num + i];
		}
	}
	std::vector<char *> t_coding(parity_num);
	char **coding = (char **)t_coding.data();
	std::vector<std::vector<char>> t_coding_area(parity_num, std::vector<char>(blocksize));
	for (int i = 0; i < parity_num; i++)
	{
		coding[i] = t_coding_area[i].data();
	}
	for (int i = 0; i < parity_num; i++)
	{
		std::vector<int> new_matrix(1 * num_of_block_each_parity, 1);
		jerasure_matrix_encode(num_of_block_each_parity, 1, 8, new_matrix.data(), &data[i * num_of_block_each_parity], &coding[i], blocksize);
	}

	std::vector<int> rs_matrix(k * m, 0);
	hpc_make_matrix(x, k, m, seri_num, rs_matrix.data());

	std::vector<int> matrix(parity_num * k, 0);
	int index = 0;
	for (auto it = parity_idx_ptrs->begin(); it != parity_idx_ptrs->end(); it++)
	{
		int j = *it;
		memcpy(&matrix[index], &rs_matrix[j * k], k * sizeof(int));
		index += k;
	}

	std::vector<int> d_matrix(parity_num * parity_num, 1);
	int idx = 0;
	for (int i = 0; i < parity_num; i++)
	{
		for (int j = 0; j < parity_num; j++)
		{
			d_matrix[idx++] = matrix[i * k + erasures[j]];
		}
	}

	// for(int i = 0; i < parity_num * parity_num; i++)
	// {
	//     std::cout << d_matrix[i] << " ";
	// }
	// std::cout << std::endl;

	std::vector<int> inverse(parity_num * parity_num);
	jerasure_invert_matrix(d_matrix.data(), inverse.data(), parity_num, 8);

	jerasure_matrix_encode(parity_num, parity_num, 8, inverse.data(), coding, repair_ptrs, blocksize);

	return true;
}

bool ECProject::perform_addition_xor(char **data_ptrs, char **coding_ptrs, int blocksize, int block_num, int parity_num)
{
	if (block_num % parity_num != 0)
	{
		printf("invalid! %d mod %d != 0\n", block_num, parity_num);
		return false;
	}
	int num_of_block_each_parity = block_num / parity_num;

	std::vector<char *> t_data(block_num);
	char **data = (char **)t_data.data();
	int cnt = 0;
	for (int i = 0; i < parity_num; i++)
	{
		for (int j = 0; j < num_of_block_each_parity; j++)
		{
			data[cnt++] = data_ptrs[j * parity_num + i];
		}
	}

	for (int i = 0; i < parity_num; i++)
	{
		std::vector<int> new_matrix(1 * num_of_block_each_parity, 1);
		jerasure_matrix_encode(num_of_block_each_parity, 1, 8, new_matrix.data(), &data[i * num_of_block_each_parity], &coding_ptrs[i], blocksize);
	}
	return true;
}

// To solve the problem of encoding partial blocks to decode data and parity blocks with surviving data and parity blocks
bool ECProject::encode_partial_blocks_for_repair_HPC(int x, int seri_num, bool rv_or_ch, int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, std::shared_ptr<std::vector<int>> data_idx_ptrs, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs)
{
	auto s_parity_idx_ptrs = std::make_shared<std::vector<int>>();
	for (auto it = data_idx_ptrs->begin(); it != data_idx_ptrs->end(); it++)
	{
		int j = *it;
		if (j >= k)
		{
			s_parity_idx_ptrs->push_back(j);
		}
	}
	int s_parity_num = int(s_parity_idx_ptrs->size());
	if (s_parity_num == 0)
	{
		return ECProject::encode_partial_block_with_data_blocks_HPC(x, seri_num, rv_or_ch, k, m, data_ptrs, coding_ptrs, blocksize, data_idx_ptrs, block_num, parity_idx_ptrs);
	}
	int s_data_num = block_num - s_parity_num;

	if (s_data_num > 0)
	{
		std::vector<char *> t_data(s_data_num);
		char **data = (char **)t_data.data();
		std::vector<char *> t_parity(s_parity_num);
		char **parity = (char **)t_parity.data();
		int cnt1 = 0, cnt2 = 0, cnt = 0;
		auto s_data_idx_ptrs = std::make_shared<std::vector<int>>();
		for (auto it = data_idx_ptrs->begin(); it != data_idx_ptrs->end(); it++)
		{
			int j = *it;
			if (j < k)
			{
				s_data_idx_ptrs->push_back(j);
				data[cnt1++] = data_ptrs[cnt++];
			}
			else
			{
				parity[cnt2++] = data_ptrs[cnt++];
			}
		}

		int parity_num = int(parity_idx_ptrs->size());
		std::vector<char *> t_coding(parity_num);
		char **coding = (char **)t_coding.data();
		std::vector<std::vector<char>> t_coding_area(s_parity_num, std::vector<char>(blocksize));

		std::vector<char *> p_data(2 * s_parity_num);
		char **datap = (char **)p_data.data();
		std::vector<char *> p_coding(s_parity_num);
		char **codingp = (char **)p_coding.data();
		cnt1 = 0;
		cnt2 = 0;
		cnt = 0;
		for (int i = 0; i < parity_num; i++)
		{
			int p_id = (*parity_idx_ptrs)[i];
			auto it = std::find(s_parity_idx_ptrs->begin(), s_parity_idx_ptrs->end(), p_id + k);
			if (it == s_parity_idx_ptrs->end())
			{
				coding[i] = coding_ptrs[i];
			}
			else
			{
				coding[i] = t_coding_area[cnt++].data();
				datap[cnt1++] = coding[i];
				codingp[cnt2++] = coding_ptrs[i];
			}
		}
		// encode partial block with only data blocks
		ECProject::encode_partial_block_with_data_blocks_HPC(x, seri_num, rv_or_ch, k, m, data, coding, blocksize, s_data_idx_ptrs, s_data_num, parity_idx_ptrs);

		for (auto it1 = parity_idx_ptrs->begin(); it1 != parity_idx_ptrs->end(); it1++)
		{
			int index = 0;
			for (auto it2 = s_parity_idx_ptrs->begin(); it2 != s_parity_idx_ptrs->end(); it2++)
			{
				if (*it1 == *it2 - k)
				{
					datap[cnt1++] = parity[index]; // parity blocks correspond to newly-encoded paritial blocks
					break;
				}
				index++;
			}
		}
		// encode partial block with surviving parity blocks and newly-encoded partial blocks
		ECProject::perform_addition_xor(datap, codingp, blocksize, 2 * s_parity_num, s_parity_num);
	}
	else
	{
		int in1 = 0;
		for (auto it1 = parity_idx_ptrs->begin(); it1 != parity_idx_ptrs->end(); it1++)
		{
			int in2 = 0;
			for (auto it2 = s_parity_idx_ptrs->begin(); it2 != s_parity_idx_ptrs->end(); it2++)
			{
				if (*it1 == *it2 - k)
				{
					memcpy(coding_ptrs[in1], data_ptrs[in2], blocksize);
					break;
				}
				in2++;
			}
			in1++;
		}
	}
	return true;
}

bool ECProject::decode_with_partial_blocks_HPC(int x, int seri_num, bool rv_or_ch, int k, int m, char **partial_or_parity, char **repair_ptrs, int blocksize, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs, int *erasures)
{
	/*  > 'PB0-1' means the partial block 0 from the cluster 1
		> 'P0' means parity block 0
		> partial block 0 should be corresponding to parity block 0
		> The order of blocks in partial_or_parity:
			> e.g. [PB0-0], [PB1-0], [PB0-1], [PB1-1], ... , [P0], [P1]
		> block_num: the total number of partial blocks and parity blocks
		> the order of erasures should be consistent with repair_ptrs
		> Produre: xor(partial + parity) -> construct matrix and invert -> encode with the inverse matrix
		> Attention! x = 1, seri_num = 0, when it's 'row and vertical' or 'column and horizontal'
	*/
	if (rv_or_ch)
	{
		x = 1;
		seri_num = 0;
	}
	int parity_num = int(parity_idx_ptrs->size());
	if (block_num % parity_num != 0)
	{
		printf("invalid! %d mod %d != 0.\nThe number of partial blocks is not matched with the number of parity blocks\n", block_num, parity_num);
		return false;
	}
	int num_of_block_each_parity = block_num / parity_num;

	std::vector<char *> t_data(block_num);
	char **data = (char **)t_data.data();
	int cnt = 0;
	for (int i = 0; i < parity_num; i++)
	{
		for (int j = 0; j < num_of_block_each_parity; j++)
		{
			data[cnt++] = partial_or_parity[j * parity_num + i];
		}
	}
	std::vector<char *> t_coding(parity_num);
	char **coding = (char **)t_coding.data();
	std::vector<std::vector<char>> t_coding_area(parity_num, std::vector<char>(blocksize));
	for (int i = 0; i < parity_num; i++)
	{
		coding[i] = t_coding_area[i].data();
	}
	for (int i = 0; i < parity_num; i++)
	{
		std::vector<int> new_matrix(1 * num_of_block_each_parity, 1);
		jerasure_matrix_encode(num_of_block_each_parity, 1, 8, new_matrix.data(), &data[i * num_of_block_each_parity], &coding[i], blocksize);
	}

	// figure out failed parity blocks
	auto failed_parity_ids = std::make_shared<std::vector<int>>();
	auto failed_data_ids = std::make_shared<std::vector<int>>();
	std::vector<int> failed_parity_e_idxs;
	std::vector<int> failed_data_e_idxs;
	for (int i = 0; i < parity_num; i++)
	{
		if (erasures[i] >= k)
		{
			failed_parity_ids->push_back(erasures[i]);
			failed_parity_e_idxs.push_back(i);
		}
		else
		{
			failed_data_ids->push_back(erasures[i]);
			failed_data_e_idxs.push_back(i);
		}
	}
	int failed_parity_num = int(failed_parity_ids->size());
	int failed_data_num = parity_num - failed_parity_num;
	if (failed_data_num == 0) // only parity repair
	{
		for (int i = 0; i < failed_parity_num; i++)
		{
			int id = erasures[i] - k;
			int idx = 0;
			for (int j = 0; j < parity_num; j++)
			{
				if (id == (*parity_idx_ptrs)[j])
				{
					idx = j;
					break;
				}
			}
			memcpy(repair_ptrs[i], coding[idx], blocksize);
		}
		return true;
	}

	std::vector<int> rs_matrix(k * m, 0);
	hpc_make_matrix(x, k, m, seri_num, rs_matrix.data());

	std::vector<char *> d_coding(failed_data_num);
	char **codingd = (char **)d_coding.data();

	std::vector<int> matrix(failed_data_num * k, 0);
	int idx1 = 0, idx2 = 0;
	int index = 0;
	for (auto it = parity_idx_ptrs->begin(); it != parity_idx_ptrs->end(); it++)
	{
		int j = *it;
		auto iter = std::find(failed_parity_ids->begin(), failed_parity_ids->end(), j + k);
		if (iter == failed_parity_ids->end())
		{
			memcpy(&matrix[index], &rs_matrix[j * k], k * sizeof(int));
			index += k;
			codingd[idx1++] = coding[idx2];
		}
		idx2++;
	}

	std::vector<int> d_matrix(failed_data_num * failed_data_num, 1);
	int idx = 0;
	for (int i = 0; i < failed_data_num; i++)
	{
		for (int j = 0; j < failed_data_num; j++)
		{
			d_matrix[idx++] = matrix[i * k + (*failed_data_ids)[j]];
		}
	}

	std::vector<char *> d_repair(failed_data_num);
	char **repaird = (char **)d_repair.data();
	for (int i = 0; i < failed_data_num; i++)
	{
		repaird[i] = repair_ptrs[failed_data_e_idxs[i]];
		// std::cout << "Repair index : " << failed_data_e_idxs[i] << std::endl;
	}

	// for(int i = 0; i < failed_data_num * failed_data_num; i++)
	// {
	//     std::cout << d_matrix[i] << " ";
	// }
	// std::cout << std::endl;

	std::vector<int> inverse(failed_data_num * failed_data_num);
	jerasure_invert_matrix(d_matrix.data(), inverse.data(), failed_data_num, 8);
	// repair datablock
	jerasure_matrix_encode(failed_data_num, failed_data_num, 8, inverse.data(), codingd, repaird, blocksize);

	if (failed_parity_num > 0)
	{
		auto f_parity_idx_ptrs = std::make_shared<std::vector<int>>();
		std::vector<char *> p_coding(2 * failed_parity_num);
		char **codingp = (char **)p_coding.data();
		std::vector<char *> p_repair(failed_parity_num);
		char **repairp = (char **)p_repair.data();
		std::vector<int> matrix(failed_data_num * k, 0);
		int in1 = 0, in2 = 0;
		for (auto it = parity_idx_ptrs->begin(); it != parity_idx_ptrs->end(); it++)
		{
			int j = *it;
			auto iter = std::find(failed_parity_ids->begin(), failed_parity_ids->end(), j + k);
			if (iter != failed_parity_ids->end())
			{
				f_parity_idx_ptrs->push_back(j);
				int t_idx = int(iter - failed_parity_ids->begin());
				codingp[in1] = coding[in2];
				repairp[in1] = repair_ptrs[failed_parity_e_idxs[t_idx]];
				// std::cout << "Repair index : " << failed_parity_e_idxs[t_idx] <<std::endl;
				in1++;
			}
			in2++;
		}
		// encode partial block with repaired data block
		std::vector<std::vector<char>> p_coding_area(failed_parity_num, std::vector<char>(blocksize));
		for (int i = 0; i < failed_parity_num; i++)
		{
			codingp[i + failed_parity_num] = p_coding_area[i].data();
		}
		ECProject::encode_partial_block_with_data_blocks_HPC(x, seri_num, rv_or_ch, k, m, repaird, &codingp[failed_parity_num], blocksize, failed_data_ids, failed_data_num, f_parity_idx_ptrs);

		ECProject::perform_addition_xor(codingp, repairp, blocksize, 2 * failed_parity_num, failed_parity_num);
	}

	return true;
}

bool ECProject::encode_partial_block_with_data_blocks_PC(int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, std::shared_ptr<std::vector<int>> data_idx_ptrs, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs)
{
	/*
		> Only support encoding partial blocks with only data blocks
	*/
	int *rs_matrix = reed_sol_vandermonde_coding_matrix(k, m, 8);
	int parity_num = int(parity_idx_ptrs->size());
	std::vector<int> matrix(parity_num * k, 0);
	int cnt = 0;
	for (auto it = parity_idx_ptrs->begin(); it != parity_idx_ptrs->end(); it++)
	{
		int j = *it;
		memcpy(&matrix[cnt * k], &rs_matrix[j * k], k * sizeof(int));
		cnt++;
	}

	std::vector<int> new_matrix(parity_num * block_num, 1);

	int idx = 0;
	for (auto it = data_idx_ptrs->begin(); it != data_idx_ptrs->end(); it++)
	{
		for (int i = 0; i < parity_num; i++)
		{
			int j = *it;
			new_matrix[i * block_num + idx] = matrix[i * k + j];
		}
		idx++;
	}

	// for(int i = 0; i < parity_num * block_num; i++)
	// {
	// 	std::cout << new_matrix[i] << " ";
	// }
	// std::cout << std::endl;

	jerasure_matrix_encode(block_num, parity_num, 8, new_matrix.data(), data_ptrs, coding_ptrs, blocksize);

	return true;
}

bool ECProject::encode_partial_blocks_for_repair_PC(int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, std::shared_ptr<std::vector<int>> data_idx_ptrs, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs)
{
	auto s_parity_idx_ptrs = std::make_shared<std::vector<int>>();
	for (auto it = data_idx_ptrs->begin(); it != data_idx_ptrs->end(); it++)
	{
		int j = *it;
		if (j >= k)
		{
			s_parity_idx_ptrs->push_back(j);
		}
	}
	int s_parity_num = int(s_parity_idx_ptrs->size());
	if (s_parity_num == 0)
	{
		return ECProject::encode_partial_block_with_data_blocks_PC(k, m, data_ptrs, coding_ptrs, blocksize, data_idx_ptrs, block_num, parity_idx_ptrs);
	}
	int s_data_num = block_num - s_parity_num;

	if (s_data_num > 0)
	{
		std::vector<char *> t_data(s_data_num);
		char **data = (char **)t_data.data();
		std::vector<char *> t_parity(s_parity_num);
		char **parity = (char **)t_parity.data();
		int cnt1 = 0, cnt2 = 0, cnt = 0;
		auto s_data_idx_ptrs = std::make_shared<std::vector<int>>();
		for (auto it = data_idx_ptrs->begin(); it != data_idx_ptrs->end(); it++)
		{
			int j = *it;
			if (j < k)
			{
				s_data_idx_ptrs->push_back(j);
				data[cnt1++] = data_ptrs[cnt++];
			}
			else
			{
				parity[cnt2++] = data_ptrs[cnt++];
			}
		}

		int parity_num = int(parity_idx_ptrs->size());
		std::vector<char *> t_coding(parity_num);
		char **coding = (char **)t_coding.data();
		std::vector<std::vector<char>> t_coding_area(s_parity_num, std::vector<char>(blocksize));

		std::vector<char *> p_data(2 * s_parity_num);
		char **datap = (char **)p_data.data();
		std::vector<char *> p_coding(s_parity_num);
		char **codingp = (char **)p_coding.data();
		cnt1 = 0;
		cnt2 = 0;
		cnt = 0;
		for (int i = 0; i < parity_num; i++)
		{
			int p_id = (*parity_idx_ptrs)[i];
			auto it = std::find(s_parity_idx_ptrs->begin(), s_parity_idx_ptrs->end(), p_id + k);
			if (it == s_parity_idx_ptrs->end())
			{
				coding[i] = coding_ptrs[i];
			}
			else
			{
				coding[i] = t_coding_area[cnt++].data();
				datap[cnt1++] = coding[i];
				codingp[cnt2++] = coding_ptrs[i];
			}
		}
		// encode partial block with only data blocks
		ECProject::encode_partial_block_with_data_blocks_PC(k, m, data, coding, blocksize, s_data_idx_ptrs, s_data_num, parity_idx_ptrs);

		for (auto it1 = parity_idx_ptrs->begin(); it1 != parity_idx_ptrs->end(); it1++)
		{
			int index = 0;
			for (auto it2 = s_parity_idx_ptrs->begin(); it2 != s_parity_idx_ptrs->end(); it2++)
			{
				if (*it1 == *it2 - k)
				{
					datap[cnt1++] = parity[index]; // parity blocks correspond to newly-encoded paritial blocks
					break;
				}
				index++;
			}
		}
		// encode partial block with surviving parity blocks and newly-encoded partial blocks
		ECProject::perform_addition_xor(datap, codingp, blocksize, 2 * s_parity_num, s_parity_num);
	}
	else
	{
		int in1 = 0;
		for (auto it1 = parity_idx_ptrs->begin(); it1 != parity_idx_ptrs->end(); it1++)
		{
			int in2 = 0;
			for (auto it2 = s_parity_idx_ptrs->begin(); it2 != s_parity_idx_ptrs->end(); it2++)
			{
				if (*it1 == *it2 - k)
				{
					memcpy(coding_ptrs[in1], data_ptrs[in2], blocksize);
					break;
				}
				in2++;
			}
			in1++;
		}
	}
	return true;
}

bool ECProject::decode_with_partial_blocks_PC(int k, int m, char **partial_or_parity, char **repair_ptrs, int blocksize, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs, int *erasures)
{
	/*  > 'PB0-1' means the partial block 0 from the cluster 1
		> 'P0' means parity block 0
		> partial block 0 should be corresponding to parity block 0
		> The order of blocks in partial_or_parity:
			> e.g. [PB0-0], [PB1-0], [PB0-1], [PB1-1], ... , [P0], [P1]
		> block_num: the total number of partial blocks and parity blocks
		> the order of erasures should be consistent with repair_ptrs
		> Produre: xor(partial + parity) -> construct matrix and invert -> encode with the inverse matrix
	*/
	int parity_num = int(parity_idx_ptrs->size());
	if (block_num % parity_num != 0)
	{
		printf("invalid! %d mod %d != 0.\nThe number of partial blocks is not matched with the number of parity blocks\n", block_num, parity_num);
		return false;
	}
	int num_of_block_each_parity = block_num / parity_num;

	std::vector<char *> t_data(block_num);
	char **data = (char **)t_data.data();
	int cnt = 0;
	for (int i = 0; i < parity_num; i++)
	{
		for (int j = 0; j < num_of_block_each_parity; j++)
		{
			data[cnt++] = partial_or_parity[j * parity_num + i];
		}
	}
	std::vector<char *> t_coding(parity_num);
	char **coding = (char **)t_coding.data();
	std::vector<std::vector<char>> t_coding_area(parity_num, std::vector<char>(blocksize));
	for (int i = 0; i < parity_num; i++)
	{
		coding[i] = t_coding_area[i].data();
	}
	for (int i = 0; i < parity_num; i++)
	{
		std::vector<int> new_matrix(1 * num_of_block_each_parity, 1);
		jerasure_matrix_encode(num_of_block_each_parity, 1, 8, new_matrix.data(), &data[i * num_of_block_each_parity], &coding[i], blocksize);
	}

	// figure out failed parity blocks
	auto failed_parity_ids = std::make_shared<std::vector<int>>();
	auto failed_data_ids = std::make_shared<std::vector<int>>();
	std::vector<int> failed_parity_e_idxs;
	std::vector<int> failed_data_e_idxs;
	for (int i = 0; i < parity_num; i++)
	{
		if (erasures[i] >= k)
		{
			failed_parity_ids->push_back(erasures[i]);
			failed_parity_e_idxs.push_back(i);
		}
		else
		{
			failed_data_ids->push_back(erasures[i]);
			failed_data_e_idxs.push_back(i);
		}
	}
	int failed_parity_num = int(failed_parity_ids->size());
	int failed_data_num = parity_num - failed_parity_num;
	if (failed_data_num == 0) // only parity repair
	{
		for (int i = 0; i < failed_parity_num; i++)
		{
			int id = erasures[i] - k;
			int idx = 0;
			for (int j = 0; j < parity_num; j++)
			{
				if (id == (*parity_idx_ptrs)[j])
				{
					idx = j;
					break;
				}
			}
			memcpy(repair_ptrs[i], coding[idx], blocksize);
		}
		return true;
	}

	int *rs_matrix = reed_sol_vandermonde_coding_matrix(k, m, 8);

	std::vector<char *> d_coding(failed_data_num);
	char **codingd = (char **)d_coding.data();

	std::vector<int> matrix(failed_data_num * k, 0);
	int idx1 = 0, idx2 = 0;
	int index = 0;
	for (auto it = parity_idx_ptrs->begin(); it != parity_idx_ptrs->end(); it++)
	{
		int j = *it;
		auto iter = std::find(failed_parity_ids->begin(), failed_parity_ids->end(), j + k);
		if (iter == failed_parity_ids->end())
		{
			memcpy(&matrix[index], &rs_matrix[j * k], k * sizeof(int));
			index += k;
			codingd[idx1++] = coding[idx2];
		}
		idx2++;
	}

	std::vector<int> d_matrix(failed_data_num * failed_data_num, 1);
	int idx = 0;
	for (int i = 0; i < failed_data_num; i++)
	{
		for (int j = 0; j < failed_data_num; j++)
		{
			d_matrix[idx++] = matrix[i * k + (*failed_data_ids)[j]];
		}
	}

	std::vector<char *> d_repair(failed_data_num);
	char **repaird = (char **)d_repair.data();
	for (int i = 0; i < failed_data_num; i++)
	{
		repaird[i] = repair_ptrs[failed_data_e_idxs[i]];
		// std::cout << "Repair index : " << failed_data_e_idxs[i] << std::endl;
	}

	// for(int i = 0; i < failed_data_num * failed_data_num; i++)
	// {
	//     std::cout << d_matrix[i] << " ";
	// }
	// std::cout << std::endl;

	std::vector<int> inverse(failed_data_num * failed_data_num);
	jerasure_invert_matrix(d_matrix.data(), inverse.data(), failed_data_num, 8);
	// repair datablock
	jerasure_matrix_encode(failed_data_num, failed_data_num, 8, inverse.data(), codingd, repaird, blocksize);

	if (failed_parity_num > 0)
	{
		auto f_parity_idx_ptrs = std::make_shared<std::vector<int>>();
		std::vector<char *> p_coding(2 * failed_parity_num);
		char **codingp = (char **)p_coding.data();
		std::vector<char *> p_repair(failed_parity_num);
		char **repairp = (char **)p_repair.data();
		std::vector<int> matrix(failed_data_num * k, 0);
		int in1 = 0, in2 = 0;
		for (auto it = parity_idx_ptrs->begin(); it != parity_idx_ptrs->end(); it++)
		{
			int j = *it;
			auto iter = std::find(failed_parity_ids->begin(), failed_parity_ids->end(), j + k);
			if (iter != failed_parity_ids->end())
			{
				f_parity_idx_ptrs->push_back(j);
				int t_idx = int(iter - failed_parity_ids->begin());
				codingp[in1] = coding[in2];
				repairp[in1] = repair_ptrs[failed_parity_e_idxs[t_idx]];
				// std::cout << "Repair index : " << failed_parity_e_idxs[t_idx] <<std::endl;
				in1++;
			}
			in2++;
		}
		// encode partial block with repaired data block
		std::vector<std::vector<char>> p_coding_area(failed_parity_num, std::vector<char>(blocksize));
		for (int i = 0; i < failed_parity_num; i++)
		{
			codingp[i + failed_parity_num] = p_coding_area[i].data();
		}
		ECProject::encode_partial_block_with_data_blocks_PC(k, m, repaird, &codingp[failed_parity_num], blocksize, failed_data_ids, failed_data_num, f_parity_idx_ptrs);

		ECProject::perform_addition_xor(codingp, repairp, blocksize, 2 * failed_parity_num, failed_parity_num);
	}

	return true;
}