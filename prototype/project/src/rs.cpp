#include <rs.h>
#include <lrc.h>

bool ECProject::encode_RS(int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize)
{
	int *rs_matrix = reed_sol_vandermonde_coding_matrix(k, m, 8);
	jerasure_matrix_encode(k, m, 8, rs_matrix, data_ptrs, coding_ptrs, blocksize);
	free(rs_matrix);
	return true;
}

bool ECProject::decode_RS(int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, int *erasures, int failed_num)
{
	if(failed_num > m)
	{
		std::cout << "[Decode] Undecodable!" << std::endl;
		return false;
	}
	int *rs_matrix = reed_sol_vandermonde_coding_matrix(k, m, 8);
	int i = 0;
	i = jerasure_matrix_decode(k, m, 8, rs_matrix, failed_num, erasures, data_ptrs, coding_ptrs, blocksize);
	if(i == -1)
	{
		std::cout << "[Decode] Failed!" << std::endl;
	}
	return true;
}

bool ECProject::encode_partial_blocks_with_data_blocks_RS(int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, std::shared_ptr<std::vector<int>> data_idx_ptrs, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs)
{
    int *rs_matrix = reed_sol_vandermonde_coding_matrix(k, m, 8);
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

bool ECProject::encode_partial_blocks_for_repair_RS(int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, std::shared_ptr<std::vector<int>> data_idx_ptrs, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs)
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
		return ECProject::encode_partial_blocks_with_data_blocks_RS(k, m, data_ptrs, coding_ptrs, blocksize, data_idx_ptrs, block_num, parity_idx_ptrs);
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
        ECProject::encode_partial_blocks_with_data_blocks_RS(k, m, data, coding, blocksize, s_data_idx_ptrs, s_data_num, parity_idx_ptrs);
        
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

bool ECProject::decode_with_partial_blocks_RS(int k, int m, char **partial_or_parity, char **repair_ptrs, int blocksize, int block_num, std::shared_ptr<std::vector<int>> parity_idx_ptrs, int *erasures)
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
	
    int *rs_matrix = reed_sol_vandermonde_coding_matrix(k, m, 8);

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
		ECProject::encode_partial_blocks_with_data_blocks_RS(k, m, repaird, &codingp[failed_parity_num], blocksize, failed_data_ids, failed_data_num, f_parity_idx_ptrs);
		
		ECProject::perform_addition(codingp, repairp, blocksize, 2 * failed_parity_num, failed_parity_num);
	}
	
	return true;
}