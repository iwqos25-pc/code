#include <pc.h>
#include <set>
#include <vector>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <sys/stat.h>

int main(int argc, char **argv)
{
    char buff[256];
    getcwd(buff, 256);
    std::string cwf = std::string(argv[0]);

    int k1 = 4, m1 = 2, k2 = 2, m2 = 1;
    int objects_num = 2;  // <10
    int block_size = 128;
    int value_length = 1;

    int parity_blocks_number = k2 * m1 + k1 * m2 + m1 * m2;
    std::vector<char *> v_data(k1 * k2);
    std::vector<char *> v_coding(parity_blocks_number);
    char **data = (char **)v_data.data();
    char **coding = (char **)v_coding.data();
    std::vector<std::vector<char>> coding_area(parity_blocks_number, std::vector<char>(block_size));
    std::string readdir = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../../data/";
    std::string targetdir = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../../storage/";
    if (access(targetdir.c_str(), 0) == -1)
    {
        mkdir(targetdir.c_str(), S_IRWXU);
    }
    char *value = new char[value_length * 1024];

    // Storage
    for(int j = 0; j < objects_num; j++)
    {
        std::string key;
        if (j < 10)
        {
          key = "Object0" + std::to_string(j);
        }
        else
        {
          key = "Object" + std::to_string(j);
        }
        std::string readpath = readdir + key;
        
        std::ifstream ifs(readpath);
        ifs.read(value, value_length * 1024);
        ifs.close();
        char *p_value = const_cast<char *>(value);
        for (int i = 0; i < k1 * k2; i++)
        {
            data[i] = p_value + i * block_size;
        }
        for (int i = 0; i < parity_blocks_number; i++)
        {
            coding[i] = coding_area[i].data();
        } 
        // ECProject::encode_PC(k1, m1, k2, m2, data, coding, block_size);
        ECProject::encode_HPC(2, k1, m1, k2, m2, data, coding, block_size, true, j);
        for(int i = 0; i < k1 * k2 + parity_blocks_number; i++)
        {
            std::string block_key;
            block_key = key + "_" + std::to_string(i);
            std::string writepath = targetdir + block_key;
            std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
            if(i < k1 * k2)
            {
                ofs.write(data[i], block_size);
            }
            else
            {
                ofs.write(coding[i - k1 * k2], block_size);
            }
            ofs.flush();
            ofs.close();
        }
    }
    delete value;

    // Get and Repair
    auto c1_data_block_idxs = std::make_shared<std::vector<int>>();
    auto c2_data_block_idxs = std::make_shared<std::vector<int>>();
    auto c3_parity_block_idxs = std::make_shared<std::vector<int>>();
    c1_data_block_idxs->push_back(0);
    c2_data_block_idxs->push_back(2);
    c3_parity_block_idxs->push_back(0);
    c1_data_block_idxs->push_back(3);
    c2_data_block_idxs->push_back(1);
    c3_parity_block_idxs->push_back(1);
    auto parity_idx_ptrs = std::make_shared<std::vector<int>>();
    parity_idx_ptrs->push_back(0);
    parity_idx_ptrs->push_back(1);

    std::vector<char *> t_coding(3);
    char **codingt = (char **)t_coding.data();
    std::vector<std::vector<char>> t_coding_area(3, std::vector<char>(block_size));
    for(int i = 0; i < 3; i++)
    {
        codingt[i] = t_coding_area[i].data();
    }
    std::vector<char *> p_coding(4);
    char **codingp = (char **)p_coding.data();
    std::vector<std::vector<char>> p_coding_area(4, std::vector<char>(block_size));
    for(int i = 0; i < 4; i++)
    {
        codingp[i] = p_coding_area[i].data();
    }
    std::vector<char *> s_data(4);
    char **datas = (char **)s_data.data();
    std::vector<char *> p_data(6);
    char **datap = (char **)p_data.data();

    
    // cluster 1
    std::vector<char *> c1_data(2);
    char **data1 = (char **)c1_data.data();
    std::vector<std::vector<char>> c1_data_area(2, std::vector<char>(block_size));
    int i = 0;
    for(auto it = c1_data_block_idxs->begin(); it != c1_data_block_idxs->end(); it++, i++)
    {
        int idx = *it;
        std::string key;
        key = "Object00_" + std::to_string(idx);
        std::string readpath = targetdir + key;
        data1[i] = c1_data_area[i].data();
        datas[(*it)] = data1[i];
        std::ifstream ifs(readpath);
        ifs.read(data1[i], block_size);
        ifs.close();
    }
    c1_data_block_idxs->pop_back();
    ECProject::encode_partial_blocks_for_repair_HPC(2, 0, true, k1, m1, data1, &codingp[0], block_size, c1_data_block_idxs, 1, parity_idx_ptrs);
    datap[0] = codingp[0];
    datap[1] = codingp[1];

    // cluster 2
    std::vector<char *> c2_data(3);
    char **data2 = (char **)c2_data.data();
    std::vector<std::vector<char>> c2_data_area(2, std::vector<char>(block_size));
    i = 0;
    for(auto it = c2_data_block_idxs->begin(); it != c2_data_block_idxs->end(); it++, i++)
    {
        int idx = *it;
        std::string key;
        key = "Object00_" + std::to_string(idx);
        std::string readpath = targetdir + key;
        data2[i] = c2_data_area[i].data();
        datas[(*it)] = data2[i];
        std::ifstream ifs(readpath);
        ifs.read(data2[i], block_size);
        ifs.close();
    }
    c2_data_block_idxs->pop_back();
    ECProject::encode_partial_blocks_for_repair_HPC(2, 0, true, k1, m1, data2, &codingp[2], block_size, c2_data_block_idxs, 1, parity_idx_ptrs);
    datap[2] = codingp[2];
    datap[3] = codingp[3];

    // cluster 3
    for(auto it = c3_parity_block_idxs->begin(); it != c3_parity_block_idxs->end(); it++, i++)
    {
        int idx = *it + k1 * k2;
        std::string key;
        key = "Object00_" + std::to_string(idx);
        std::string readpath = targetdir + key;
        std::ifstream ifs(readpath);
        ifs.read(codingt[(*it)], block_size);
        ifs.close();
    }
    // data2[2] = codingt[0];
    // c2_data_block_idxs->push_back(4);
    // ECProject::encode_partial_blocks_for_repair_HPC(2, 0, true, k1, m1, data2, &codingp[2], block_size, c2_data_block_idxs, 3, parity_idx_ptrs);
    // datap[2] = codingp[2];
    // datap[3] = codingp[3];
    datap[4] = codingt[0];
    // memset(codingt[2], 0, block_size);
    datap[5] = codingt[1];

    int *erasures = new int[3];
    // int failed_idx = 3;
    int failed_cnt = 2;
    erasures[0] = 1;
    erasures[1] = 3;
    erasures[2] = -1;
    std::vector<char *> t_repaired(2);
    char **repaired = (char **)t_repaired.data();
    for(int i = 0; i < failed_cnt; i++)
    {
        if(erasures[i] > 3)
        {
            printf("Before lost:\nBlock %d : %s\n", i, codingt[erasures[i] - 4]);
            memset(codingt[erasures[i] - 4], 0, block_size);
            repaired[i] = codingt[erasures[i] - 4];
        }
        else
        {
            printf("Before lost:\nBlock %d : %s\n", i, datas[erasures[i]]);
            memset(datas[erasures[i]], 0, block_size);
            repaired[i] = datas[erasures[i]];
        }
    }
    for(int i = 0; i < failed_cnt; i++)
    {
        if(erasures[i] > 3)
            printf("After lost:\nBlock %d : %s\n", i, codingt[erasures[i] - 4]);
        else
            printf("After lost:\nBlock %d : %s\n", i, datas[erasures[i]]);
    }
    // ECProject::decode_by_row_or_col(4, 2, datas, codingt, block_size, erasures, 2);
    ECProject::decode_with_partial_blocks_HPC(2, 0, true, k1, m1, datap, repaired, block_size, 6, parity_idx_ptrs, erasures);
    // ECProject::decode_with_partial_blocks_HPC(2, 0, true, k1, m1, &datap[2], repaired, block_size, 4, parity_idx_ptrs, erasures);
    for(int i = 0; i < failed_cnt; i++)
    {
        if(erasures[i] > 3)
            printf("Repair:\nBlock %d : %s\n", i, codingt[erasures[i] - 4]);
        else
            printf("Repair:\nBlock %d : %s\n", i, datas[erasures[i]]);
    }
    
    int *rs_matrix = reed_sol_vandermonde_coding_matrix(2 * k2, m2, 8);
    for(int i = 0; i < 2 * k2 * m2; i++)
    {
        std::cout << rs_matrix[i] << " ";
    }
    std::cout << std::endl;
    rs_matrix = reed_sol_vandermonde_coding_matrix(k1, m1, 8);
    for(int i = 0; i < k1 * m1; i++)
    {
        std::cout << rs_matrix[i] << " ";
    }
    std::cout << std::endl;
    rs_matrix = reed_sol_vandermonde_coding_matrix(2 * k1, m1, 8);
    for(int i = 0; i < 2 * k1 * m1; i++)
    {
        std::cout << rs_matrix[i] << " ";
    }
    std::cout << std::endl;
    std::vector<int> f_matrix(k1 * m1, 0);
    ECProject::hpc_make_matrix(2, k1, m1, 0, f_matrix.data());
    for(int i = 0; i < k1 * m1; i++)
    {
        std::cout << f_matrix[i] << " ";
    }
    std::cout << std::endl;



    return 0;
}