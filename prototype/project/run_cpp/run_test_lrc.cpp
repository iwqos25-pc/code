#include <lrc.h>
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

    int k = 4;
    int l = 2;
    int g = 2;
    int objects_num = 2; // <10
    int block_size = 256;
    int value_length = 1;
    ECProject::EncodeType encode_type = ECProject::Azure_LRC;

    int parity_blocks_number = g + l;
    std::vector<char *> v_data(k);
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
        for (int i = 0; i < k; i++)
        {
            data[i] = p_value + i * block_size;
        }
        for (int i = 0; i < parity_blocks_number; i++)
        {
            coding[i] = coding_area[i].data();
        }
        ECProject::encode_LRC(k, g, l, data, coding, block_size, ECProject::Azure_LRC);
        for(int i = 0; i < k + g + l; i++)
        {
            std::string block_key;
            block_key = key + "_" + std::to_string(i);
        std::string writepath = targetdir + block_key;
        std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
        if(i < k)
        {
          ofs.write(data[i], block_size);
        }
        else
        {
          ofs.write(coding[i - k], block_size);
        }
        ofs.flush();
        ofs.close();
        }
    }

    delete value;

    // Get and Merge
    auto local_data_block_idxs = std::make_shared<std::vector<int>>();
    auto g1_data_block_idxs = std::make_shared<std::vector<int>>();
    auto g2_data_block_idxs = std::make_shared<std::vector<int>>();
    local_data_block_idxs->push_back(0);
    g1_data_block_idxs->push_back(1);
    g1_data_block_idxs->push_back(2);
    g1_data_block_idxs->push_back(3);
    g2_data_block_idxs->push_back(4);
    local_data_block_idxs->push_back(5);
    g1_data_block_idxs->push_back(6);
    g2_data_block_idxs->push_back(7);

    std::vector<char *> t_coding(6);
    char **codingt = (char **)t_coding.data();
    std::vector<std::vector<char>> t_coding_area(6, std::vector<char>(block_size));
    for(int i = 0; i < 6; i++)
    {
        codingt[i] = t_coding_area[i].data();
    }
    std::vector<char *> s_data(8);
    char **datas = (char **)s_data.data();

    // partial 1
    std::vector<char *> g1_data(4);
    char **data1 = (char **)g1_data.data();
    std::vector<std::vector<char>> g1_data_area(4, std::vector<char>(block_size));
    int i = 0;
    for(auto it = g1_data_block_idxs->begin(); it != g1_data_block_idxs->end(); it++, i++)
    {
        int idx = *it;
        if(idx > 4)
        {
            idx -= 4;
        }
        std::string key;
        key = "Object0" + std::to_string((*it)/4) + "_" + std::to_string(idx);
        std::string readpath = targetdir + key;
        data1[i] = g1_data_area[i].data();
        datas[(*it)] = data1[i];
        std::ifstream ifs(readpath);
        ifs.read(data1[i], block_size);
        ifs.close();
    }
    ECProject::encode_partial_blocks(2 * k, g, data1, codingt, block_size, g1_data_block_idxs, 4, encode_type);

    // partial 2
    std::vector<char *> g2_data(2);
    char **data2 = (char **)g2_data.data();
    std::vector<std::vector<char>> g2_data_area(2, std::vector<char>(block_size));
    i = 0;
    for(auto it = g2_data_block_idxs->begin(); it != g2_data_block_idxs->end(); it++, i++)
    {
        int idx = *it;
        if(idx > 4)
        {
            idx -= 4;
        }
        std::string key;
        key = "Object0" + std::to_string((*it)/4) + "_" + std::to_string(idx);
        std::string readpath = targetdir + key;
        data2[i] = g2_data_area[i].data();
        datas[(*it)] = data2[i];
        std::ifstream ifs(readpath);
        ifs.read(data2[i], block_size);
        ifs.close();
    }
    ECProject::encode_partial_blocks(2 * k, g, data2, &codingt[2], block_size, g2_data_block_idxs, 2, encode_type);

    // main recalculate
    std::vector<char *> m_data(2);
    char **datam = (char **)m_data.data();
    std::vector<std::vector<char>> m_data_area(2, std::vector<char>(block_size));
    i = 0;
    for(auto it = local_data_block_idxs->begin(); it != local_data_block_idxs->end(); it++, i++)
    {
        int idx = *it;
        if(idx > 4)
        {
            idx -= 4;
        }
        std::string key;
        key = "Object0" + std::to_string((*it)/4) + "_" + std::to_string(idx);
        std::string readpath = targetdir + key;
        datam[i] = m_data_area[i].data();
        datas[(*it)] = datam[i];
        std::ifstream ifs(readpath);
        ifs.read(datam[i], block_size);
        ifs.close();
    }
    ECProject::encode_partial_blocks(2 * k, g, datam, &codingt[4], block_size, local_data_block_idxs, 2, encode_type);

    std::vector<char *> tt_coding(6);
    char **codingtt = (char **)tt_coding.data();
    codingtt[0] = codingt[0];
    codingtt[1] = codingt[2];
    codingtt[2] = codingt[4];
    codingtt[3] = codingt[1];
    codingtt[4] = codingt[3];
    codingtt[5] = codingt[5];

    std::vector<char *> g_coding(2);
    char **codingg = (char **)g_coding.data();
    std::vector<std::vector<char>> g_coding_area(2, std::vector<char>(block_size));
    for(i = 0; i < 2; i++)
    {
        codingg[i] = g_coding_area[i].data();
    }
    ECProject::perform_addition(codingtt, codingg, block_size, 6, 2);

    // Check if decodable
    int *erasures = new int[3];
    int failed_idx = 5;
    printf("Before lost:\n%s\n", datas[failed_idx]);
    memset(datas[failed_idx], 0, block_size);
    printf("After lost:\n%s\n", datas[failed_idx]);
    erasures[0] = failed_idx;
    erasures[1] = -1;
    int *matrix = reed_sol_vandermonde_coding_matrix(2 * k, g, 8);
    i = 0;
    i = jerasure_matrix_decode(2 * k, g, 8, matrix, 1, erasures, datas, codingg, block_size);
    printf("Repaired:\n%s\n", datas[failed_idx]);
    if(i == -1)
    {
        printf("Unsuccess!\n");
    }
    else
    {
      printf("Decode success!\n");
    }
        
      return 0;
}