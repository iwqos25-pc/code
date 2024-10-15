#include "client.h"
#include "toolbox.h"
#include <fstream>
#include <sys/time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cfloat>
#include <cstring>

inline void rand_n_num(int min, int max, int n, std::vector<int> &random_numbers)
{
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dis(min, max);

  int cnt = 0;
  int num = dis(gen);
  random_numbers.push_back(num);
  cnt++;
  while (cnt < n)
  {
    while (std::find(random_numbers.begin(), random_numbers.end(), num) != random_numbers.end())
    {
      num = dis(gen);
    }
    random_numbers.push_back(num);
    cnt++;
  }
}

inline int rand_num(int min, int max)
{
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dis(min, max);
  int num = dis(gen);
  return num;
};

template <typename T>
void getArrayStats(T arr[], int len, T &avg, T &max, T &min)
{
  if (len == 0)
  {
    return;
  }
  max = -1;
  min = T(INT_MAX);
  T sum = 0;
  for (int i = 0; i < len; i++)
  {
    sum += arr[i];
    if (arr[i] > max)
    {
      max = arr[i];
    }
    if (arr[i] < min)
    {
      min = arr[i];
    }
  }
  avg = sum / len;
}

void print_s_result(double cost[], double d_cost[], int cross_rack[], double failed_block_num[], double meta_t[], double cross_rt[], double inner_rt[], int r, int avg_num, int block_size)
{
  std::cout << "   <Simulation, Total> Cross Rack: " << cross_rack[r] << " blocks." << std::endl;
  std::cout << "   <Simulation, Average> Cross Rack: " << cross_rack[r] / avg_num << " blocks." << std::endl;
  std::cout << "   Total Cost: " << cost[r] << "s, (D_cost)" << d_cost[r] << "s, (meta)" << meta_t[r]
            << "s, (cross)" << cross_rt[r] << "s, (inner)" << inner_rt[r] << "s." << std::endl;
  std::cout << "   Average Cost: " << cost[r] / avg_num << "s, (D_cost)" << d_cost[r] / avg_num
            << "s, (meta)" << meta_t[r] / avg_num << "s, (cross)" << cross_rt[r] / avg_num
            << "s, (inner)" << inner_rt[r] / avg_num << "s." << std::endl;
  if (failed_block_num[r] > 0 && block_size > 0)
  {
    double repair_rates = failed_block_num[r] * double(block_size) / cost[r];
    std::cout << "   Failed block: " << failed_block_num[r] << "." << std::endl;
    std::cout << "   Repair Rate: " << repair_rates << " MiB/s." << std::endl;
  }
}

void print_result(double cost[], double d_cost[], int cross_rack[], double failed_block_num[], double meta_t[], double cross_rt[], double inner_rt[], int len, int avg_num, int block_size)
{
  double max_cost = 0.0, min_cost = 0.0, avg_cost = 0.0;
  double max_dcost = 0.0, min_dcost = 0.0, avg_dcost = 0.0;
  double max_failed = 0.0, min_failed = 0.0, avg_failed = 0.0;
  int max_cross_rack = 0, min_cross_rack = 0, avg_cross_rack = 0;
  double max_meta_t = 0.0, min_meta_t = 0.0, avg_meta_t = 0.0;
  double max_cross_rt = 0.0, min_cross_rt = 0.0, avg_cross_rt = 0.0;
  double max_inner_rt = 0.0, min_inner_rt = 0.0, avg_inner_rt = 0.0;
  getArrayStats(cost, len, avg_cost, max_cost, min_cost);
  getArrayStats(d_cost, len, avg_dcost, max_dcost, min_dcost);
  getArrayStats(cross_rack, len, avg_cross_rack, max_cross_rack, min_cross_rack);
  getArrayStats(failed_block_num, len, avg_failed, max_failed, min_failed);
  getArrayStats(meta_t, len, avg_meta_t, max_meta_t, min_meta_t);
  getArrayStats(cross_rt, len, avg_cross_rt, max_cross_rt, min_cross_rt);
  getArrayStats(inner_rt, len, avg_inner_rt, max_inner_rt, min_inner_rt);
  std::cout << "Average Result:" << std::endl;
  std::cout << "   <Simulation, Total> Cross Rack: (" << avg_cross_rack << ", " << max_cross_rack << ", " << min_cross_rack << ")blocks." << std::endl;
  std::cout << "   <Simulation, Average> Cross Rack: (" << avg_cross_rack / avg_num << ", " << max_cross_rack / avg_num << ", "
            << min_cross_rack / avg_num << ")blocks." << std::endl;
  std::cout << "   Total Cost: Average: " << avg_cost << "s, Max: " << max_cost << "s, Min: " << min_cost << "s." << std::endl;
  std::cout << "   Average Cost: Average: " << avg_cost / avg_num << "s, Max: " << max_cost / avg_num << "s, Min: " << min_cost / avg_num << "s." << std::endl;
  std::cout << "   Total Decode Cost: Average: " << avg_dcost << "s, Max: " << max_dcost << "s, Min: " << min_dcost << "s." << std::endl;
  std::cout << "   Average Decode Cost: Average: " << avg_dcost / avg_num << "s, Max: " << max_dcost / avg_num << "s, Min: " << min_dcost / avg_num << "s." << std::endl;
  std::cout << "   Total Meta Cost: Average: " << avg_meta_t << "s, Max: " << max_meta_t << "s, Min: " << min_meta_t << "s." << std::endl;
  std::cout << "   Average Meta Cost: Average: " << avg_meta_t / avg_num << "s, Max: " << max_meta_t / avg_num << "s, Min: " << min_meta_t / avg_num << "s." << std::endl;
  std::cout << "   Total Cross-rack Cost: Average: " << avg_cross_rt << "s, Max: " << max_cross_rt << "s, Min: " << min_cross_rt << "s." << std::endl;
  std::cout << "   Average Cross-rack Cost: Average: " << avg_cross_rt / avg_num << "s, Max: " << max_cross_rt / avg_num << "s, Min: " << min_cross_rt / avg_num << "s." << std::endl;
  std::cout << "   Total Inner-rack Cost: Average: " << avg_inner_rt << "s, Max: " << max_inner_rt << "s, Min: " << min_inner_rt << "s." << std::endl;
  std::cout << "   Average Inner-rack Cost: Average: " << avg_inner_rt / avg_num << "s, Max: " << max_inner_rt / avg_num << "s, Min: " << min_inner_rt / avg_num << "s." << std::endl;
  if (avg_failed > 0 && block_size > 0)
  {
    double avg_repair_rates = avg_failed * double(block_size) / avg_cost;
    double max_repair_rates = max_failed * double(block_size) / max_cost;
    double min_repair_rates = min_failed * double(block_size) / min_cost;
    std::cout << "   Failed Block: Average: " << avg_failed << ", Max: " << max_failed << ", Min: " << min_failed << "." << std::endl;
    std::cout << "   Repair Rate: Average: " << avg_repair_rates << "s, Max: " << max_repair_rates << "s, Min: " << min_repair_rates << "." << std::endl;
  }
}

int main(int argc, char **argv)
{
  if (argc != 13 && argc != 14 && argc != 15)
  {
    std::cout << "e.g. 0" << std::endl;
    std::cout << "./run_client rack_num node_num_in_rack coordinator_ip encode_type multistripes_placement_type partial_decoding stripe_num value_length x k m false" << std::endl;
    std::cout << "./run_client 5 10 0.0.0.0 RS DIS false 32 1024 2 8 2 false" << std::endl;
    std::cout << "e.g. 1" << std::endl;
    std::cout << "./run_client rack_num node_num_in_rack coordinator_ip encode_type multistripes_placement_type partial_decoding stripe_num value_length x k l g false" << std::endl;
    std::cout << "./run_client 5 10 0.0.0.0 Azure_LRC DIS false 32 1024 2 8 2 2 false" << std::endl;
    std::cout << "e.g. 2" << std::endl;
    std::cout << "./run_client rack_num node_num_in_rack coordinator_ip encode_type multistripes_placement_type partial_decoding stripe_num value_length x k1 m1 k2 m2 false" << std::endl;
    std::cout << "./run_client 5 10 0.0.0.0 HPC Vertical false 32 1024 2 3 1 2 1 false" << std::endl;
    exit(-1);
  }

  bool partial_decoding, approach;
  ECProject::EncodeType encode_type;
  ECProject::SingleStripePlacementType s_placement_type;
  ECProject::MultiStripesPlacementType m_placement_type;
  int k, l, g_m;
  int k1, m1, k2, m2;
  int stripe_num, value_length;
  int x;
  int rack_num, node_num_in_rack;
  std::string coordinator_ip;

  char buff[256];
  getcwd(buff, 256);
  std::string cwf = std::string(argv[0]);

  rack_num = std::stoi(std::string(argv[1]));
  node_num_in_rack = std::stoi(std::string(argv[2]));
  coordinator_ip = std::string(argv[3]);
  if (std::string(argv[4]) == "Azure_LRC")
  {
    encode_type = ECProject::Azure_LRC;
  }
  else if (std::string(argv[4]) == "HPC")
  {
    encode_type = ECProject::HPC;
  }
  else if (std::string(argv[4]) == "RS")
  {
    encode_type = ECProject::RS;
  }
  else if (std::string(argv[4]) == "MLEC")
  {
    encode_type = ECProject::MLEC;
  }
  else
  {
    std::cout << "error: unknown encode_type" << std::endl;
    exit(-1);
  }

  s_placement_type = ECProject::Optimal;

  if (std::string(argv[5]) == "Ran")
  {
    m_placement_type = ECProject::Ran;
  }
  else if (std::string(argv[5]) == "DIS")
  {
    m_placement_type = ECProject::DIS;
  }
  else if (std::string(argv[5]) == "AGG")
  {
    m_placement_type = ECProject::AGG;
  }
  else if (std::string(argv[5]) == "OPT")
  {
    m_placement_type = ECProject::OPT;
  }
  else if (std::string(argv[5]) == "Vertical")
  {
    m_placement_type = ECProject::Vertical;
  }
  else if (std::string(argv[5]) == "Horizontal")
  {
    m_placement_type = ECProject::Horizontal;
  }
  else
  {
    std::cout << "error: unknown singlestripe_placement_type" << std::endl;
    exit(-1);
  }
  partial_decoding = (std::string(argv[6]) == "true");
  stripe_num = std::stoi(std::string(argv[7]));
  value_length = std::stoi(std::string(argv[8]));
  x = std::stoi(std::string(argv[9]));
  int block_num = 0;
  if (argc == 13 && encode_type != ECProject::HPC)
  {
    k = std::stoi(std::string(argv[10]));
    g_m = std::stoi(std::string(argv[11]));
    l = k / g_m;
    approach = (std::string(argv[12]) == "true");
    block_num = k + g_m;
  }
  else if (argc == 14 && (encode_type != ECProject::HPC || encode_type != ECProject::MLEC))
  {
    k = std::stoi(std::string(argv[10]));
    l = std::stoi(std::string(argv[11]));
    g_m = std::stoi(std::string(argv[12]));
    approach = (std::string(argv[13]) == "true");
    block_num = k + l + g_m;
  }
  else if (argc == 15 && (encode_type == ECProject::HPC || encode_type == ECProject::MLEC))
  {
    k1 = std::stoi(std::string(argv[10]));
    m1 = std::stoi(std::string(argv[11]));
    k2 = std::stoi(std::string(argv[12]));
    m2 = std::stoi(std::string(argv[13]));
    approach = (std::string(argv[14]) == "true");
    k = k1 * k2;
    block_num = (k1 + m1) * (k2 + m2);
  }
  else
  {
    std::cout << "error: invalid parameters!" << std::endl;
    exit(-1);
  }

  std::string client_ip = "0.0.0.0";

  ECProject::Client client(client_ip, 44444, coordinator_ip + std::string(":55555"));
  std::cout << client.sayHelloToCoordinatorByGrpc("Client") << std::endl;

  if (stripe_num > 100)
  {
    std::cout << "Do not support stripe number greater than 100!" << std::endl;
    exit(-1);
  }

  if (encode_type == ECProject::HPC || encode_type == ECProject::MLEC)
  {
    if (client.SetParameterByGrpc({partial_decoding, approach, encode_type, s_placement_type, m_placement_type, k1, m1, k2, m2, x}))
    {
      std::cout << "set parameter successfully!" << std::endl;
    }
    else
    {
      std::cout << "Failed to set parameter!" << std::endl;
    }
  }
  else
  {
    if (client.SetParameterByGrpc({partial_decoding, approach, encode_type, s_placement_type, m_placement_type, k, l, g_m, x}))
    {
      std::cout << "set parameter successfully!" << std::endl;
    }
    else
    {
      std::cout << "Failed to set parameter!" << std::endl;
    }
  }

  std::unordered_map<std::string, std::string> key_values;
  /*生成随机的key value对*/

  struct timeval s_start_time, s_end_time;
  // struct timeval g_start_time, g_end_time;
  double s_time = 0.0, g_time = 0.0;
  // set
  std::cout << "[SET BEGIN]" << std::endl;
  gettimeofday(&s_start_time, NULL);
  for (int i = 0; i < stripe_num; i++)
  {
    std::string key;
    if (i < 10)
    {
      key = "Object0" + std::to_string(i);
    }
    else
    {
      key = "Object" + std::to_string(i);
    }
    std::string readpath = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../../data/Object";
    if (access(readpath.c_str(), 0) == -1)
    {
      std::cout << "[Client] file does not exist!" << std::endl;
      exit(-1);
    }
    else
    {
      char *buf = new char[value_length * 1024];
      std::ifstream ifs(readpath);
      ifs.read(buf, value_length * 1024);
      client.set(key, std::string(buf));
      ifs.close();
      delete buf;
    }
  }
  gettimeofday(&s_end_time, NULL);
  std::cout << "[SET END]" << std::endl
            << std::endl;

  // get
  // std::cout << "[GET BEGIN]" << std::endl;
  // gettimeofday(&g_start_time, NULL);
  // for (int i = 0; i < stripe_num; i++)
  // {
  //   std::string value;
  //   std::string key;
  //   if (i < 10)
  //   {
  //     key = "Object0" + std::to_string(i);
  //   }
  //   else
  //   {
  //     key = "Object" + std::to_string(i);
  //   }
  //   client.get(key, value);
  // }
  // gettimeofday(&g_end_time, NULL);
  // std::cout << "[GET END]" << std::endl
  //           << std::endl;

  s_time = s_end_time.tv_sec - s_start_time.tv_sec + (s_end_time.tv_usec - s_start_time.tv_usec) * 1.0 / 1000000;
  // g_time = g_end_time.tv_sec - g_start_time.tv_sec + (g_end_time.tv_usec - g_start_time.tv_usec) * 1.0 / 1000000;
  std::cout << "Throughput:" << std::endl;
  std::cout << "Write: " << float(stripe_num * value_length) / float(1024 * s_time) << "MB/s, time =" << s_time << "s." << std::endl;
  // std::cout << "Read: " << float(stripe_num * value_length) / float(1024 * g_time) << "MB/s, time =" << g_time << "s." << std::endl;

  std::cout << "Number of stripes(objects): " << stripe_num << std::endl;
  std::cout << "Object size: " << value_length << "KiB" << std::endl;
  std::cout << "Block size: " << float(value_length) / float(k) << "KiB" << std::endl;
  if (encode_type == ECProject::HPC)
  {
    std::cout << "HPC(x, k1, m1, k2, m2): HPC(" << x << ", " << k1 << ", " << m1 << ", " << k2 << ", " << m2 << ")" << std::endl;
  }
  else if (encode_type == ECProject::MLEC)
  {
    std::cout << "MLEC(k1, m1, k2, m2): MLEC(" << k1 << ", " << m1 << ", " << k2 << ", " << m2 << ")" << std::endl;
  }
  else if (encode_type == ECProject::Azure_LRC)
  {
    std::cout << "x " << std::string(argv[5]) << "(k, l, g): " << x << std::string(argv[5]) << "(" << k << ", " << l << ", " << g_m << ")" << std::endl;
  }
  else
  {
    std::cout << "x RS(k, m): " << x << "RS(" << k << ", " << g_m << ")" << std::endl;
  }
  if (m_placement_type == ECProject::Ran && partial_decoding)
  {
    std::cout << "Placement type: Ran-P" << std::endl;
  }
  else
  {
    std::cout << "Placement type: " << std::string(argv[6]) << std::endl;
  }
  std::cout << "Other: ";
  if (partial_decoding)
  {
    std::cout << "encode-and-transfer, \n";
  }
  std::cout << std::endl;

  // repair, average
  std::cout << "[REPAIR BEGIN]" << std::endl;
  int tot_node_num = rack_num * node_num_in_rack;
  int block_size = value_length / (k * 1024);
  double temp_cost = 0.0;
  double temp_d_cost = 0.0;
  double temp_meta_t = 0.0, temp_cross_rt = 0.0, temp_inner_rt = 0.0;
  int temp_failed = 0;
  int temp_cross_rack = 0;

  double cost[15] = {0};
  double d_cost[15] = {0};
  int cross_rack[15] = {0};
  double failed_block_num[15] = {0};
  double inner_rack_time[15] = {0};
  double cross_rack_time[15] = {0};
  double meta_time[15] = {0};
  int run_time = 5;
  // single-block repair
  std::cout << "---------------------------------------------" << std::endl;
  std::cout << "Before merging. Single-Block-Failure Repair:" << std::endl;
  for (int r = 0; r < stripe_num; r++)
  {
    std::cout << "Runtime " << r + 1 << ":" << std::endl;
    for (int j = 0; j < block_num; j++)
    {
      std::vector<int> failed_block_list;
      failed_block_list.push_back(j);
      temp_failed = client.block_repair(failed_block_list, r, temp_cost, temp_d_cost, temp_cross_rack, temp_meta_t, temp_cross_rt, temp_inner_rt);
      cost[r] += temp_cost;
      d_cost[r] += temp_d_cost;
      cross_rack[r] += temp_cross_rack;
      failed_block_num[r] += double(temp_failed);
      meta_time[r] += temp_meta_t;
      inner_rack_time[r] += temp_inner_rt;
      cross_rack_time[r] += temp_cross_rt;
    }
    print_s_result(cost, d_cost, cross_rack, failed_block_num, meta_time, cross_rack_time, inner_rack_time, r, block_num, 0);
  }
  print_result(cost, d_cost, cross_rack, failed_block_num, meta_time, cross_rack_time, inner_rack_time, stripe_num, block_num, 0);
  std::cout << "---------------------------------------------" << std::endl;

  // merge
  std::cout << "[MERGE BEGIN]" << std::endl;
  double merge_cost = client.merge(x);
  std::cout << "Merge Cost: " << merge_cost << std::endl;
  std::cout << "[MERGE END]" << std::endl
            << std::endl;

  if(encode_type == ECProject::HPC || encode_type == ECProject::MLEC)
  {
    if (std::string(argv[5]) == "Horizontal")
    {
      block_num = (x * k1 + m1) * (k2 + m2);
    }
    else
    {
      block_num = (k1 + m1) * (x * k2 + m2);
    }
  }
  else if(encode_type == ECProject::Azure_LRC)
  {
    block_num = x * k + g_m + l;
  }
  else if(encode_type == ECProject::RS)
  {
    block_num = x * k + g_m;
  }

  std::cout << "---------------------------------------------" << std::endl;
  std::cout << "After merging. Single-Block-Failure Repair:" << std::endl;
  for (int r = stripe_num; r < stripe_num + stripe_num / x; r++)
  {
    std::cout << "Runtime " << r + 1 << ":" << std::endl;
    
    for (int j = 0; j < block_num; j++)
    {
      std::vector<int> failed_block_list;
      failed_block_list.push_back(j);
      temp_failed = client.block_repair(failed_block_list, r, temp_cost, temp_d_cost, temp_cross_rack, temp_meta_t, temp_cross_rt, temp_inner_rt);
      cost[r] += temp_cost;
      d_cost[r] += temp_d_cost;
      cross_rack[r] += temp_cross_rack;
      failed_block_num[r] += double(temp_failed);
      meta_time[r] += temp_meta_t;
      inner_rack_time[r] += temp_inner_rt;
      cross_rack_time[r] += temp_cross_rt;
    }
    print_s_result(cost, d_cost, cross_rack, failed_block_num, meta_time, cross_rack_time, inner_rack_time, r, block_num, 0);
  }
  print_result(cost, d_cost, cross_rack, failed_block_num, meta_time, cross_rack_time, inner_rack_time, stripe_num, block_num, 0);
  std::cout << "---------------------------------------------" << std::endl;

  std::cout << "[DEL BEGIN]" << std::endl;
  client.delete_all_stripes();
  std::cout << "[DEL END]" << std::endl
            << std::endl;
  return 0;
}