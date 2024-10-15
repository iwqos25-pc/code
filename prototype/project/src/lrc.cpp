#include <lrc.h>

void ECProject::dfs(std::vector<int> temp, std::shared_ptr<std::vector<std::vector<int>>> ans, int cur, int n, int k)
{
    if (int(temp.size()) + (n - cur + 1) < k)
    {
        return;
    }
    if (int(temp.size()) == k)
    {
        ans->push_back(temp);
        return;
    }
    temp.push_back(cur);
    dfs(temp, ans, cur + 1, n, k);
    temp.pop_back();
    dfs(temp, ans, cur + 1, n, k);
}

bool ECProject::combine(std::shared_ptr<std::vector<std::vector<int>>> ans, int n, int k)
{
    std::vector<int> temp;
    dfs(temp, ans, 1, n, k);
    return true;
}

// check if any data block is failed
bool ECProject::check_k_data(std::vector<int> erasures, int k)
{
    int flag = 1;
    for (int i = 0; i < k; i++)
    {
        if (std::find(erasures.begin(), erasures.end(), i) != erasures.end())
        {
            flag = 0;
        }
    }
    if (flag)
    {
        return true;
    }

    return false;
}

// generate encoding matrix for lrc
bool ECProject::lrc_make_matrix(int k, int g, int real_l, int *final_matrix, EncodeType encode_type)
{
    int r = (k + real_l - 1) / real_l;
    int *matrix = NULL;

    if (encode_type == Azure_LRC){
        matrix = reed_sol_vandermonde_coding_matrix(k, g + 1, 8); //(k, g, w)
    }
    
    if (matrix == NULL)
    {
        std::cout << "matrix == NULL" << std::endl;
    }

    if (final_matrix == NULL)
    {
        std::cout << "final_matrix == NULL" << std::endl;
    }
    bzero(final_matrix, sizeof(int) * k * (g + real_l));

    for (int i = 0; i < g; i++)
    {
        for (int j = 0; j < k; j++)
        {
            final_matrix[i * k + j] = matrix[(i + 1) * k + j];
        }
    }

    for (int i = 0; i < real_l; i++)
    {
        for (int j = 0; j < k; j++)
        {
            if (i * r <= j && j < (i + 1) * r)
            {
                final_matrix[(i + g) * k + j] = 1;
            }
        }
    }

    free(matrix);
    return true;
}

// encode
bool ECProject::encode_LRC(int k, int g_m, int real_l, char **data_ptrs, char **coding_ptrs, int blocksize, EncodeType encode_type)
{
    std::vector<int> new_matrix((g_m + real_l) * k, 0);
    lrc_make_matrix(k, g_m, real_l, new_matrix.data(), encode_type);
    jerasure_matrix_encode(k, g_m + real_l, 8, new_matrix.data(), data_ptrs, coding_ptrs, blocksize);
    return true;
}

bool ECProject::decode_lrc(int k, int g_m, int real_l, char **data_ptrs, char **coding_ptrs, int blocksize, int *erasures, int failed_num)
{
    std::vector<int> new_matrix((g_m + real_l) * k, 0);
    lrc_make_matrix(k, g_m, real_l, new_matrix.data(), Azure_LRC);
    int i = 0;
	i = jerasure_matrix_decode(k, g_m + real_l, 8, new_matrix.data(), failed_num, erasures, data_ptrs, coding_ptrs, blocksize);
	if(i == -1)
	{
		std::cout << "[Decode] Failed!" << std::endl;
        return false;
	}
    return true;
}

// decode
bool ECProject::decode(int k, int g_m, int real_l, char **data_ptrs, char **coding_ptrs, std::shared_ptr<std::vector<int>> erasures, int blocksize, EncodeType encode_type, bool repair)
{

    if (encode_type == Azure_LRC)
    {
        std::vector<int> matrix((g_m + real_l) * k, 0);
        lrc_make_matrix(k, g_m, real_l, matrix.data(), encode_type);
        if (!repair)
        {
            if (check_k_data(*erasures, k)) // if there is no failed data block, return true
            {
                return true;
            }
        }
        // decode the original data blocks by any k blocks from the stripe
        if (jerasure_matrix_decode(k, g_m + real_l, 8, matrix.data(), 0, erasures->data(), data_ptrs, coding_ptrs, blocksize) == -1)
        {
            std::vector<int> new_erasures(g_m + real_l + 1, 1);
            int survival_number = k + g_m + real_l - erasures->size() + 1;
            std::vector<int> survival_index;
            auto part_new_erasure = std::make_shared<std::vector<std::vector<int>>>();
            for (int i = 0; i < int(erasures->size() - 1); i++)
            {
                new_erasures[i] = (*erasures)[i];
            }
            new_erasures[g_m + real_l] = -1;

            for (int i = 0; i < k + g_m + real_l; i++)
            {
                if (std::find(erasures->begin(), erasures->end(), i) == erasures->end())
                {
                    survival_index.push_back(i);
                }
            }
            if (survival_number > k)
            {
                combine(part_new_erasure, survival_index.size(), survival_number - k);
            }
            for (int i = 0; i < int(part_new_erasure->size()); i++)
            {
                for (int j = 0; j < int((*part_new_erasure)[i].size()); j++)
                {
                    new_erasures[erasures->size() - 1 + j] = survival_index[(*part_new_erasure)[i][j] - 1];
                }

                if (jerasure_matrix_decode(k, g_m + real_l, 8, matrix.data(), 0, new_erasures.data(), data_ptrs, coding_ptrs, blocksize) != -1)
                {
                    return true;
                    break;
                }
            }
        }
        else
        {
            return true;
        }
        std::cout << "undecodable!!!!!!!!!!!!" << std::endl;
    }
    return false;
}

bool ECProject::check_received_block(int k, int expect_block_number, std::shared_ptr<std::vector<int>> shards_idx_ptr, int shards_ptr_size)
{
    if (shards_ptr_size != -1)
    {
        if (int(shards_idx_ptr->size()) != shards_ptr_size)
        {
            return false;
        }
    }

    if (int(shards_idx_ptr->size()) >= expect_block_number)
    {
        return true;
    }
    else if (int(shards_idx_ptr->size()) == k) // azure_lrc防误杀
    {
        for (int i = 0; i < k; i++)
        {
            // 没找到
            if (std::find(shards_idx_ptr->begin(), shards_idx_ptr->end(), i) == shards_idx_ptr->end())
            {
                return false;
            }
        }
    }
    else
    {
        return false;
    }
    return true;
}
int ECProject::check_decodable_azure_lrc(int k, int g, int l, std::vector<int> failed_block, std::vector<int> new_matrix)
{
    // 数据块，全局校验块，局部校验块
    // 检查是否满足理论可解
    std::vector<int> survive_block;
    for (int i = 0; i < k + l + g; i++)
    {
        if (std::find(failed_block.begin(), failed_block.end(), i) == failed_block.end())
        {
            survive_block.push_back(i);
        }
    }
    if (survive_block.size() != size_t(k))
    {
        return -2;
    }
    std::set<int> group_number;
    for (int block_index : survive_block)
    {
        group_number.insert(block_index / l);
    }
    if (survive_block.size() > g + group_number.size())
    {
        return -1;
    }

    std::vector<int> matrix((k + g + l) * k, 0);
    for (int i = 0; i < k; i++)
    {
        matrix[i * k + i] = 1;
    }
    for (int i = 0; i < (g + l) * k; i++)
    {
        matrix[k * k + i] = new_matrix[i];
    }
    std::vector<int> k_k_matrix(k * k, 0);

    for (size_t i = 0; i < survive_block.size(); i++)
    {
        for (int j = 0; j < k; j++)
        {
            k_k_matrix[i * k + j] = matrix[survive_block[i] * k + j];
        }
    }
    if (jerasure_invertible_matrix(k_k_matrix.data(), k, 8) == 0)
    {
        return -1;
    }
    return 1;
}

bool ECProject::encode_partial_blocks(int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, std::shared_ptr<std::vector<int>> data_idx_ptrs, int block_num, EncodeType encode_type)
{
    int *rs_matrix = NULL;
    if(encode_type == Azure_LRC || encode_type == RS)
    {
        rs_matrix = reed_sol_vandermonde_coding_matrix(k, m, 8);
    }
    
    std::vector<int> matrix(m * k, 0);
    memcpy(matrix.data(), rs_matrix, m * k * sizeof(int));
    free(rs_matrix);
    
    std::vector<int> new_matrix(m * block_num, 1);

    int idx = 0;
    for(auto it = data_idx_ptrs->begin(); it != data_idx_ptrs->end(); it++)
    {
        for(int i = 0; i < m; i++)
        {
            int j = *it;
            new_matrix[i * block_num + idx] = matrix[i * k + j];
        }
        idx++;
    }

    jerasure_matrix_encode(block_num, m, 8, new_matrix.data(), data_ptrs, coding_ptrs, blocksize);

    return true;
}

bool ECProject::perform_addition(char **data_ptrs, char **coding_ptrs, int blocksize, int block_num, int parity_num)
{
    if(block_num % parity_num != 0)
    {
        printf("invalid! %d mod %d != 0\n", block_num, parity_num);
        return false;
    }
    int num_of_block_each_parity = block_num / parity_num;

    std::vector<char *> t_data(block_num);
    char **data = (char **)t_data.data();
	int cnt = 0;
	for(int i = 0; i < parity_num; i++)
	{
		for(int j = 0; j < num_of_block_each_parity; j++)
		{
			data[cnt++] = data_ptrs[j * parity_num + i];
		}
	}

    for(int i = 0; i < parity_num; i++)
    {
        std::vector<int> new_matrix(1 * num_of_block_each_parity, 1);
        jerasure_matrix_encode(num_of_block_each_parity, 1, 8, new_matrix.data(), &data_ptrs[i * num_of_block_each_parity], &coding_ptrs[i], blocksize);
    }
    return true;
}

bool ECProject::encode_partial_blocks_with_data_blocks_lrc(int k, int m, int real_l, char **data_ptrs, char **coding_ptrs, int blocksize, std::shared_ptr<std::vector<int>> data_idx_ptrs, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs)
{
    std::vector<int> rs_matrix((m + real_l) * k, 0);
    lrc_make_matrix(k, m, real_l, rs_matrix.data(), Azure_LRC);
    int parity_num = int(parity_idx_ptrs->size());
	std::vector<int> matrix(parity_num * k, 0);
	int cnt = 0;
	for(auto it = parity_idx_ptrs->begin(); it != parity_idx_ptrs->end(); it++)
	{
		int j = *it;
		memcpy(&matrix[cnt * k], &rs_matrix[j * k], k * sizeof(int));
		cnt++;
	}
    
    std::vector<int> new_matrix(parity_num * block_num, 1);

    int idx = 0;
    for(auto it = data_idx_ptrs->begin(); it != data_idx_ptrs->end(); it++)
    {
        for(int i = 0; i < parity_num; i++)
        {
            int j = *it;
            new_matrix[i * block_num + idx] = matrix[i * k + j];
        }
        idx++;
    }

    jerasure_matrix_encode(block_num, parity_num, 8, new_matrix.data(), data_ptrs, coding_ptrs, blocksize);
    return true;
}

bool ECProject::decode_with_partial_blocks_lrc(int k, int m, int real_l, char **partial_or_parity, char **repair_ptrs, int blocksize, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs, int *erasures)
{
    int parity_num = int(parity_idx_ptrs->size());
	if(block_num % parity_num != 0)
    {
        printf("invalid! %d mod %d != 0.\nThe number of partial blocks is not matched with the number of parity blocks\n", block_num, parity_num);
        return false;
    }
	int num_of_block_each_parity = block_num / parity_num;

	std::vector<char *> t_data(block_num);
    char **data = (char **)t_data.data();
	int cnt = 0;
	for(int i = 0; i < parity_num; i++)
	{
		for(int j = 0; j < num_of_block_each_parity; j++)
		{
			data[cnt++] = partial_or_parity[j * num_of_block_each_parity + i];
		}
	}
	std::vector<char *> t_coding(parity_num);
    char **coding = (char **)t_coding.data();
    std::vector<std::vector<char>> t_coding_area(parity_num, std::vector<char>(blocksize));
    for(int i = 0; i < parity_num; i++)
	{
		coding[i] = t_coding_area[i].data();
	}
	for(int i = 0; i < parity_num; i++)
	{
		std::vector<int> new_matrix(1 * num_of_block_each_parity, 1);
        jerasure_matrix_encode(num_of_block_each_parity, 1, 8, new_matrix.data(), &data[i * num_of_block_each_parity], &coding[i], blocksize);
	}

	std::vector<int> rs_matrix((m + real_l) * k, 0);
    lrc_make_matrix(k, m, real_l, rs_matrix.data(), Azure_LRC);

	std::vector<int> matrix(parity_num * k, 0);
	int index = 0;
	for(auto it = parity_idx_ptrs->begin(); it != parity_idx_ptrs->end(); it++)
	{
		int j = *it;
		memcpy(&matrix[index], &rs_matrix[j * k], k * sizeof(int));
		index += k;
	}

	std::vector<int> d_matrix(parity_num * parity_num, 1);
	int idx = 0;
	for(int i = 0; i < parity_num; i++)
	{
		for(int j = 0; j < parity_num; j++)
		{
			d_matrix[idx++] = matrix[i * k + erasures[j]];
		}
	}

	std::vector<int> inverse(parity_num * parity_num);
	jerasure_invert_matrix(d_matrix.data(), inverse.data(), parity_num, 8);
	
	jerasure_matrix_encode(parity_num, parity_num, 8, inverse.data(), coding, repair_ptrs, blocksize);
    return true;
}

bool ECProject::encode_partial_blocks_for_repair_LRC(int k, int m, int real_l, char **data_ptrs, char **coding_ptrs, int blocksize, std::shared_ptr<std::vector<int>> data_idx_ptrs, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs)
{
    auto s_parity_idx_ptrs = std::make_shared<std::vector<int>>();
	for(auto it = data_idx_ptrs->begin(); it != data_idx_ptrs->end(); it++)
	{
		int j = *it;
		if(j >= k)
		{
			s_parity_idx_ptrs->push_back(j);
		}
	}
	int s_parity_num = int(s_parity_idx_ptrs->size());
	if(s_parity_num == 0)
	{
		return ECProject::encode_partial_blocks_with_data_blocks_lrc(k, m, real_l, data_ptrs, coding_ptrs, blocksize, data_idx_ptrs, block_num, parity_idx_ptrs);
	}
	int s_data_num = block_num - s_parity_num;
    
    if(s_data_num > 0)
    {
        std::vector<char *> t_data(s_data_num);
        char **data = (char **)t_data.data();
        std::vector<char *>t_parity(s_parity_num);
        char **parity = (char **)t_parity.data();
        int cnt1 = 0, cnt2 = 0, cnt = 0;
        auto s_data_idx_ptrs = std::make_shared<std::vector<int>>();
        for(auto it = data_idx_ptrs->begin(); it != data_idx_ptrs->end(); it++)
        {
            int j = *it;
            if(j < k)
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
        // std::cout << parity_num << " " << s_data_num << " " << s_parity_num << std::endl;
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
        for(int i = 0; i < parity_num; i++)
        {
            int p_id = (*parity_idx_ptrs)[i];
            auto it = std::find(s_parity_idx_ptrs->begin(), s_parity_idx_ptrs->end(), p_id + k);
            if(it == s_parity_idx_ptrs->end())
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
        ECProject::encode_partial_blocks_with_data_blocks_lrc(k, m, real_l, data, coding, blocksize, s_data_idx_ptrs, s_data_num, parity_idx_ptrs);
        
        for(auto it1 = parity_idx_ptrs->begin(); it1 != parity_idx_ptrs->end(); it1++)
        {
            int index = 0;
            for(auto it2 = s_parity_idx_ptrs->begin(); it2 != s_parity_idx_ptrs->end(); it2++)
            {
                if(*it1 == *it2 - k)
                {
                    datap[cnt1++] = parity[index];	// parity blocks correspond to newly-encoded paritial blocks
                    break;
                }
                index++;
            }
        }
        // encode partial block with surviving parity blocks and newly-encoded partial blocks
        ECProject::perform_addition(datap, codingp, blocksize, 2 * s_parity_num, s_parity_num);
    }
    else
    {
        int in1 = 0;
        for(auto it1 = parity_idx_ptrs->begin(); it1 != parity_idx_ptrs->end(); it1++)
        {
            int in2 = 0;
            for(auto it2 = s_parity_idx_ptrs->begin(); it2 != s_parity_idx_ptrs->end(); it2++)
            {
                if(*it1 == *it2 - k)
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

bool ECProject::decode_with_partial_blocks_LRC(int k, int m, int real_l, char **partial_or_parity, char **repair_ptrs, int blocksize, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs, int *erasures)
{
    int parity_num = int(parity_idx_ptrs->size());
	if(block_num % parity_num != 0)
    {
        printf("invalid! %d mod %d != 0.\nThe number of partial blocks is not matched with the number of parity blocks\n", block_num, parity_num);
        return false;
    }
	int num_of_block_each_parity = block_num / parity_num;

	std::vector<char *> t_data(block_num);
    char **data = (char **)t_data.data();
	int cnt = 0;
	for(int i = 0; i < parity_num; i++)
	{
		for(int j = 0; j < num_of_block_each_parity; j++)
		{
			data[cnt++] = partial_or_parity[j * parity_num + i];
		}
	}
	std::vector<char *> t_coding(parity_num);
    char **coding = (char **)t_coding.data();
    std::vector<std::vector<char>> t_coding_area(parity_num, std::vector<char>(blocksize));
    for(int i = 0; i < parity_num; i++)
	{
		coding[i] = t_coding_area[i].data();
	}
	for(int i = 0; i < parity_num; i++)
	{
		std::vector<int> new_matrix(1 * num_of_block_each_parity, 1);
        jerasure_matrix_encode(num_of_block_each_parity, 1, 8, new_matrix.data(), &data[i * num_of_block_each_parity], &coding[i], blocksize);
	}
	
	// figure out failed parity blocks
	auto failed_parity_ids = std::make_shared<std::vector<int>>();
	auto failed_data_ids = std::make_shared<std::vector<int>>();
	std::vector<int> failed_parity_e_idxs;
	std::vector<int> failed_data_e_idxs;
	for(int i = 0; i < parity_num; i++)
	{
		if(erasures[i] >= k)
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
	if(failed_data_num == 0) // only parity repair
	{
		for(int i = 0; i < failed_parity_num; i++)
		{
			int id = erasures[i] - k;
			int idx = 0;
			for(int j = 0; j < parity_num; j++)
			{
				if(id == (*parity_idx_ptrs)[j])
				{
					idx = j;
					break;
				}
			}
			memcpy(repair_ptrs[i], coding[idx], blocksize);
		}
		return true;
	}
	
    std::vector<int> rs_matrix(k * (m + real_l), 0);
    lrc_make_matrix(k, m, real_l, rs_matrix.data(), Azure_LRC);

	std::vector<char *> d_coding(failed_data_num);
    char **codingd = (char **)d_coding.data();
	
	std::vector<int> matrix(failed_data_num * k, 0);
	int idx1 = 0, idx2 = 0;
	int index = 0;
	for(auto it = parity_idx_ptrs->begin(); it != parity_idx_ptrs->end(); it++)
	{
		int j = *it;
		auto iter = std::find(failed_parity_ids->begin(), failed_parity_ids->end(), j + k);
		if(iter == failed_parity_ids->end())
		{
			memcpy(&matrix[index], &rs_matrix[j * k], k * sizeof(int));
			index += k;
			codingd[idx1++] = coding[idx2];
		}
		idx2++;
	}

	std::vector<int> d_matrix(failed_data_num * failed_data_num, 1);
	int idx = 0;
	for(int i = 0; i < failed_data_num; i++)
	{
		for(int j = 0; j < failed_data_num; j++)
		{
			d_matrix[idx++] = matrix[i * k + (*failed_data_ids)[j]];
		}
	}
	
	std::vector<char *> d_repair(failed_data_num);
    char **repaird = (char **)d_repair.data();
	for(int i = 0 ; i < failed_data_num; i++)
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
	
	if( failed_parity_num > 0)
	{
		auto f_parity_idx_ptrs = std::make_shared<std::vector<int>>();
		std::vector<char *> p_coding(2 * failed_parity_num);
		char **codingp = (char **)p_coding.data();
		std::vector<char *> p_repair(failed_parity_num);
		char **repairp = (char **)p_repair.data();
		std::vector<int> matrix(failed_data_num * k, 0);
		int in1 = 0, in2 = 0;
		for(auto it = parity_idx_ptrs->begin(); it != parity_idx_ptrs->end(); it++)
		{
			int j = *it;
			auto iter = std::find(failed_parity_ids->begin(), failed_parity_ids->end(), j + k);
			if(iter != failed_parity_ids->end())
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
		for(int i = 0; i < failed_parity_num; i++)
		{
			codingp[i + failed_parity_num] = p_coding_area[i].data();
		}
		ECProject::encode_partial_blocks_with_data_blocks_lrc(k, m, real_l, repaird, &codingp[failed_parity_num], blocksize, failed_data_ids, failed_data_num, f_parity_idx_ptrs);
		
		ECProject::perform_addition(codingp, repairp, blocksize, 2 * failed_parity_num, failed_parity_num);
	}
	
	return true;
}

    