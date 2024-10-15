#include "coordinator.h"
#include "tinyxml2.h"
#include <random>
#include <algorithm>
#include <unistd.h>
#include <sys/time.h>

template <typename T>
inline T ceil(T const &A, T const &B)
{
  return T((A + B - 1) / B);
};

inline int rand_num(int range)
{
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dis(0, range - 1);
  int num = dis(gen);
  return num;
};

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

namespace ECProject
{
  int CoordinatorImpl::generate_placement_for_rs(int stripe_id, int block_size)
  {
    Stripe &stripe_info = m_stripe_table[stripe_id];
    int k = stripe_info.k;
    int m = stripe_info.g_m;
    ECProject::SingleStripePlacementType s_placement_type = m_encode_parameters.s_stripe_placementtype;
    ECProject::MultiStripesPlacementType m_placement_type = m_encode_parameters.m_stripe_placementtype;

    // generate stripe information
    int index = stripe_info.object_keys.size() - 1;
    std::string object_key = stripe_info.object_keys[index];
    Block *blocks_info = new Block[k + m];
    for (int i = 0; i < k + m; i++)
    {
      blocks_info[i].block_size = block_size;
      blocks_info[i].map2stripe = stripe_id;
      blocks_info[i].map2key = object_key;
      if (i < k)
      {
        std::string tmp = "_D";
        if (i < 10)
          tmp = "_D0";
        blocks_info[i].block_key = object_key + tmp + std::to_string(i);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'D';
        blocks_info[i].map2group = int(i / m);
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
      else
      {
        blocks_info[i].block_key = "Stripe" + std::to_string(stripe_id) + "_P" + std::to_string(i - k);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'P';
        blocks_info[i].map2group = int(i / m);
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
    }

    // generate data placement plan
    if (s_placement_type == Optimal)
    {
      if (m_placement_type == DIS)
      {
        int required_rack_num = ceil(k + m, m);
        int idx = m_merge_groups.size() - 1;
        if (int(m_free_racks.size()) < required_rack_num || m_free_racks.empty() || idx < 0 ||
            int(m_merge_groups[idx].size()) == m_encode_parameters.x_stripepermergegroup)
        {
          m_free_racks.clear();
          m_free_racks.shrink_to_fit();
          for (int i = 0; i < m_num_of_Racks; i++)
          {
            m_free_racks.push_back(i);
          }
          std::vector<int> temp;
          temp.push_back(stripe_id);
          m_merge_groups.push_back(temp);
        }
        else
        {
          m_merge_groups[idx].push_back(stripe_id);
        }

        // for every m blocks, place them in a separate rack
        for (int i = 0; i < k + m; i += m)
        {
          // randomly select a rack
          int t_rack_id = m_free_racks[rand_num(int(m_free_racks.size()))];
          Rack &t_rack = m_rack_table[t_rack_id];
          auto iter = std::find(m_free_racks.begin(), m_free_racks.end(), t_rack_id);
          if (iter != m_free_racks.end()) // remove the selected rack from the free list
          {
            m_free_racks.erase(iter);
          }
          for (int j = i; j < i + m && j < k + m; j++)
          {
            int t_node_id = -1;
            t_node_id = randomly_select_a_node_in_rack(t_rack_id, stripe_id);
            blocks_info[j].map2rack = t_rack_id;
            blocks_info[j].map2node = t_node_id;
            update_stripe_info_in_node(true, t_node_id, stripe_id);
            t_rack.blocks.push_back(&blocks_info[j]);
            t_rack.stripes.insert(stripe_id);
            stripe_info.place2racks.insert(t_rack_id);
          }
        }
      }
    }

    if (IF_DEBUG)
    {
      std::cout << std::endl;
      std::cout << "Data placement result:" << std::endl;
      for (int j = 0; j < m_num_of_Racks; j++)
      {
        Rack &t_rack = m_rack_table[j];
        if (int(t_rack.blocks.size()) > 0)
        {
          std::cout << "Rack " << j << ": ";
          for (auto it = t_rack.blocks.begin(); it != t_rack.blocks.end(); it++)
          {
            std::cout << "[" << (*it)->block_key << ":S" << (*it)->map2stripe << "N" << (*it)->map2node << "] ";
          }
          std::cout << std::endl;
        }
      }
      std::cout << std::endl;
      std::cout << "Merge Group: ";
      for (auto it1 = m_merge_groups.begin(); it1 != m_merge_groups.end(); it1++)
      {
        std::cout << "[ ";
        for (auto it2 = (*it1).begin(); it2 != (*it1).end(); it2++)
        {
          std::cout << (*it2) << " ";
        }
        std::cout << "] ";
      }
      std::cout << std::endl;
    }

    // randomly select a rack
    int r_idx = rand_num(int(stripe_info.place2racks.size()));
    int s_rack_id = *(std::next(stripe_info.place2racks.begin(), r_idx));
    if (IF_DEBUG)
    {
      std::cout << "[SET] Select the proxy in Rack " << s_rack_id << " to encode and set!" << std::endl;
    }
    return s_rack_id;
  }

  int CoordinatorImpl::generate_placement_for_lrc(int stripe_id, int block_size)
  {
    Stripe &stripe_info = m_stripe_table[stripe_id];
    int k = stripe_info.k;
    int l = stripe_info.l;
    int g_m = stripe_info.g_m;
    int b = m_encode_parameters.b_datapergroup;
    ECProject::EncodeType encode_type = m_encode_parameters.encodetype;
    ECProject::SingleStripePlacementType s_placement_type = m_encode_parameters.s_stripe_placementtype;
    ECProject::MultiStripesPlacementType m_placement_type = m_encode_parameters.m_stripe_placementtype;

    // generate stripe information
    int index = stripe_info.object_keys.size() - 1;
    std::string object_key = stripe_info.object_keys[index];
    Block *blocks_info = new Block[k + g_m + l];
    for (int i = 0; i < k + g_m + l; i++)
    {
      blocks_info[i].block_size = block_size;
      blocks_info[i].map2stripe = stripe_id;
      blocks_info[i].map2key = object_key;
      if (i < k)
      {
        std::string tmp = "_D";
        if (i < 10)
          tmp = "_D0";
        blocks_info[i].block_key = object_key + tmp + std::to_string(i);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'D';
        blocks_info[i].map2group = int(i / b);
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
      else if (i >= k && i < k + g_m)
      {
        blocks_info[i].block_key = "Stripe" + std::to_string(stripe_id) + "_G" + std::to_string(i - k);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'G';
        blocks_info[i].map2group = l; // for Azure-LRC
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
      else
      {
        blocks_info[i].block_key = "Stripe" + std::to_string(stripe_id) + "_L" + std::to_string(i - k - g_m);
        blocks_info[i].block_id = i;
        blocks_info[i].block_type = 'L';
        blocks_info[i].map2group = i - k - g_m;
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
    }
    // Ran, AGG, OPT can be optimized to support 'placing every theta local groups in a rack'
    // now only supports 'one local group', but we don't adopt these kinds of placement schemes in this experiment
    if (encode_type == Azure_LRC)
    {
      if (s_placement_type == Optimal)
      {
        if (m_placement_type == Ran)
        {
          int idx = m_merge_groups.size() - 1;
          if (idx < 0 || int(m_merge_groups[idx].size()) == m_encode_parameters.x_stripepermergegroup)
          {
            std::vector<int> temp;
            temp.push_back(stripe_id);
            m_merge_groups.push_back(temp);
          }
          else
          {
            m_merge_groups[idx].push_back(stripe_id);
          }

          int g_rack_id = -1;
          for (int i = 0; i < l; i++)
          {
            for (int j = i * b; j < (i + 1) * b; j += g_m + 1)
            {
              bool flag = false;
              if (j + g_m + 1 >= (i + 1) * b)
                flag = true;
              // randomly select a rack
              int t_rack_id = randomly_select_a_rack(stripe_id);
              Rack &t_rack = m_rack_table[t_rack_id];
              // place every g+1 data blocks from each group to a single rack
              for (int o = j; o < j + g_m + 1 && o < (i + 1) * b; o++)
              {
                // randomly select a node in the selected rack
                int t_node_id = randomly_select_a_node_in_rack(t_rack_id, stripe_id);
                blocks_info[o].map2rack = t_rack_id;
                blocks_info[o].map2node = t_node_id;
                update_stripe_info_in_node(true, t_node_id, stripe_id);
                t_rack.blocks.push_back(&blocks_info[o]);
                t_rack.stripes.insert(stripe_id);
                stripe_info.place2racks.insert(t_rack_id);
              }
              // place local parity blocks
              if (flag)
              {
                if (j + g_m + 1 != (i + 1) * b) // b % (g + 1) != 0
                {
                  // randomly select a node in the selected rack
                  int t_node_id = randomly_select_a_node_in_rack(t_rack_id, stripe_id);
                  blocks_info[k + g_m + i].map2rack = t_rack_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  t_rack.blocks.push_back(&blocks_info[k + g_m + i]);
                  t_rack.stripes.insert(stripe_id);
                  stripe_info.place2racks.insert(t_rack_id);
                }
                else // place the local parity blocks together with global ones
                {
                  if (g_rack_id == -1) // randomly select a new rack
                  {
                    g_rack_id = randomly_select_a_rack(stripe_id);
                  }
                  Rack &g_rack = m_rack_table[g_rack_id];
                  int t_node_id = randomly_select_a_node_in_rack(g_rack_id, stripe_id);
                  blocks_info[k + g_m + i].map2rack = g_rack_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  g_rack.blocks.push_back(&blocks_info[k + g_m + i]);
                  g_rack.stripes.insert(stripe_id);
                  stripe_info.place2racks.insert(g_rack_id);
                }
              }
            }
          }
          if (g_rack_id == -1) // randomly select a new rack
          {
            g_rack_id = randomly_select_a_rack(stripe_id);
          }
          Rack &g_rack = m_rack_table[g_rack_id];
          // place the global parity blocks to the selected rack
          for (int i = 0; i < g_m; i++)
          {
            int t_node_id = randomly_select_a_node_in_rack(g_rack_id, stripe_id);
            blocks_info[k + i].map2rack = g_rack_id;
            blocks_info[k + i].map2node = t_node_id;
            update_stripe_info_in_node(true, t_node_id, stripe_id);
            g_rack.blocks.push_back(&blocks_info[k + i]);
            g_rack.stripes.insert(stripe_id);
            stripe_info.place2racks.insert(g_rack_id);
          }
        }
        else if (m_placement_type == DIS)
        {
          int theta = 1;
          int required_rack_num = ceil(b + 1, g_m + 1) * l + 1;
          if (b < g_m)
          {
            theta = g_m / b;
            required_rack_num = l / theta + 1;
          }
          int idx = m_merge_groups.size() - 1;
          if (b % (g_m + 1) == 0)
            required_rack_num -= l;
          if (int(m_free_racks.size()) < required_rack_num || m_free_racks.empty() || idx < 0 ||
              int(m_merge_groups[idx].size()) == m_encode_parameters.x_stripepermergegroup)
          {
            m_free_racks.clear();
            m_free_racks.shrink_to_fit();
            for (int i = 0; i < m_num_of_Racks; i++)
            {
              m_free_racks.push_back(i);
            }
            std::vector<int> temp;
            temp.push_back(stripe_id);
            m_merge_groups.push_back(temp);
          }
          else
          {
            m_merge_groups[idx].push_back(stripe_id);
          }

          int g_rack_id = -1;
          for (int i = 0; i < l; i += theta)
          {
            for (int j = i * b; j < (i + theta) * b; j += g_m + theta)
            {
              bool flag = false;
              if (g_m + theta >= theta * b + theta)
                flag = true;
              // randomly select a rack
              int t_rack_id = m_free_racks[rand_num(int(m_free_racks.size()))];
              auto iter = std::find(m_free_racks.begin(), m_free_racks.end(), t_rack_id);
              if (iter != m_free_racks.end()) // remove the selected rack from the free list
              {
                m_free_racks.erase(iter);
              }
              Rack &t_rack = m_rack_table[t_rack_id];
              int in1 = 0, in2 = -1;
              // place every g+1 data blocks from each group to a single rack
              for (int o = j; o < j + g_m + theta && o < (i + theta) * b && o < k; o++)
              {
                if (in1 % b == 0)
                {
                  in2++;
                }
                in1++;
                int t_node_id = randomly_select_a_node_in_rack(t_rack_id, stripe_id);
                blocks_info[o].map2rack = t_rack_id;
                blocks_info[o].map2node = t_node_id;
                update_stripe_info_in_node(true, t_node_id, stripe_id);
                t_rack.blocks.push_back(&blocks_info[o]);
                t_rack.stripes.insert(stripe_id);
                stripe_info.place2racks.insert(t_rack_id);
              }
              // place local parity blocks
              if (flag)
              {
                if (j + g_m + theta != (i + theta) * b) // b % (g + 1) != 0
                {
                  for (int ii = 0; ii < theta; ii++)
                  {
                    int t_node_id = randomly_select_a_node_in_rack(t_rack_id, stripe_id);
                    blocks_info[k + g_m + i + ii].map2rack = t_rack_id;
                    blocks_info[k + g_m + i + ii].map2node = t_node_id;
                    update_stripe_info_in_node(true, t_node_id, stripe_id);
                    t_rack.blocks.push_back(&blocks_info[k + g_m + i + ii]);
                  }
                  t_rack.stripes.insert(stripe_id);
                  stripe_info.place2racks.insert(t_rack_id);
                }
                else // place the local parity blocks together with global ones
                {
                  if (g_rack_id == -1) // randomly select a new rack
                  {
                    g_rack_id = m_free_racks[rand_num(int(m_free_racks.size()))];
                    auto iter = std::find(m_free_racks.begin(), m_free_racks.end(), g_rack_id);
                    if (iter != m_free_racks.end())
                    {
                      m_free_racks.erase(iter);
                    }
                  }
                  Rack &g_rack = m_rack_table[g_rack_id];
                  for (int ii = 0; ii < theta; ii++)
                  {
                    int t_node_id = randomly_select_a_node_in_rack(g_rack_id, stripe_id);
                    blocks_info[k + g_m + i + ii].map2rack = g_rack_id;
                    blocks_info[k + g_m + i + ii].map2node = t_node_id;
                    update_stripe_info_in_node(true, t_node_id, stripe_id);
                    g_rack.blocks.push_back(&blocks_info[k + g_m + i + ii]);
                  }
                  g_rack.stripes.insert(stripe_id);
                  stripe_info.place2racks.insert(g_rack_id);
                }
              }
            }
          }

          if (g_rack_id == -1) // randomly select a new rack
          {
            g_rack_id = m_free_racks[rand_num(int(m_free_racks.size()))];
            auto iter = std::find(m_free_racks.begin(), m_free_racks.end(), g_rack_id);
            if (iter != m_free_racks.end())
            {
              m_free_racks.erase(iter);
            }
          }
          Rack &g_rack = m_rack_table[g_rack_id];
          // place the global parity blocks to the selected rack
          for (int i = 0; i < g_m; i++)
          {
            int t_node_id = randomly_select_a_node_in_rack(g_rack_id, stripe_id);
            blocks_info[k + i].map2rack = g_rack_id;
            blocks_info[k + i].map2node = t_node_id;
            update_stripe_info_in_node(true, t_node_id, stripe_id);
            g_rack.blocks.push_back(&blocks_info[k + i]);
            g_rack.stripes.insert(stripe_id);
            stripe_info.place2racks.insert(g_rack_id);
          }
        }
        else if (m_placement_type == AGG)
        {
          int agg_racks_num = ceil(b + 1, g_m + 1) * l + 1;
          if (b % (g_m + 1) == 0)
          {
            agg_racks_num -= l;
          }
          int idx = m_merge_groups.size() - 1;
          if (idx < 0 || int(m_merge_groups[idx].size()) == m_encode_parameters.x_stripepermergegroup)
          {
            std::vector<int> temp;
            temp.push_back(stripe_id);
            m_merge_groups.push_back(temp);
            m_agg_start_rid = rand_num(m_num_of_Racks - agg_racks_num);
          }
          else
          {
            m_merge_groups[idx].push_back(stripe_id);
          }
          int t_rack_id = m_agg_start_rid - 1;
          int g_rack_id = -1;
          for (int i = 0; i < l; i++)
          {
            for (int j = i * b; j < (i + 1) * b; j += g_m + 1)
            {
              bool flag = false;
              if (j + g_m + 1 >= (i + 1) * b)
                flag = true;
              t_rack_id += 1;
              Rack &t_rack = m_rack_table[t_rack_id];
              // place every g+1 data blocks from each group to a single rack
              for (int o = j; o < j + g_m + 1 && o < (i + 1) * b; o++)
              {
                // randomly select a node in the selected rack
                int t_node_id = randomly_select_a_node_in_rack(t_rack_id, stripe_id);
                blocks_info[o].map2rack = t_rack_id;
                blocks_info[o].map2node = t_node_id;
                update_stripe_info_in_node(true, t_node_id, stripe_id);
                t_rack.blocks.push_back(&blocks_info[o]);
                t_rack.stripes.insert(stripe_id);
                stripe_info.place2racks.insert(t_rack_id);
              }
              // place local parity blocks
              if (flag)
              {
                if (j + g_m + 1 != (i + 1) * b) // b % (g + 1) != 0
                {
                  // randomly select a node in the selected rack
                  int t_node_id = randomly_select_a_node_in_rack(t_rack_id, stripe_id);
                  blocks_info[k + g_m + i].map2rack = t_rack_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  t_rack.blocks.push_back(&blocks_info[k + g_m + i]);
                  t_rack.stripes.insert(stripe_id);
                  stripe_info.place2racks.insert(t_rack_id);
                }
                else // place the local parity blocks together with global ones
                {
                  if (g_rack_id == -1)
                  {
                    g_rack_id = t_rack_id + 1;
                    t_rack_id++;
                  }
                  Rack &g_rack = m_rack_table[g_rack_id];
                  // randomly select a node in the selected rack
                  int t_node_id = randomly_select_a_node_in_rack(g_rack_id, stripe_id);
                  blocks_info[k + g_m + i].map2rack = g_rack_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  g_rack.blocks.push_back(&blocks_info[k + g_m + i]);
                  g_rack.stripes.insert(stripe_id);
                  stripe_info.place2racks.insert(g_rack_id);
                }
              }
            }
          }
          if (g_rack_id == -1)
          {
            g_rack_id = t_rack_id + 1;
          }
          Rack &g_rack = m_rack_table[g_rack_id];
          // place the global parity blocks to the selected rack
          for (int i = 0; i < g_m; i++)
          {
            // randomly select a node in the selected rack
            int t_node_id = randomly_select_a_node_in_rack(g_rack_id, stripe_id);
            blocks_info[k + i].map2rack = g_rack_id;
            blocks_info[k + i].map2node = t_node_id;
            update_stripe_info_in_node(true, t_node_id, stripe_id);
            g_rack.blocks.push_back(&blocks_info[k + i]);
            g_rack.stripes.insert(stripe_id);
            stripe_info.place2racks.insert(g_rack_id);
          }
        }
        else if (m_placement_type == OPT)
        {
          int required_rack_num = ceil(b + 1, g_m + 1) * l + 1;
          int agg_racks_num = l + 1;
          if (b % (g_m + 1) == 0)
          {
            agg_racks_num = 1;
            required_rack_num -= l;
          }
          int idx = m_merge_groups.size() - 1;
          if (int(m_free_racks.size()) < required_rack_num - agg_racks_num || m_free_racks.empty() ||
              idx < 0 || int(m_merge_groups[idx].size()) == m_encode_parameters.x_stripepermergegroup)
          {
            m_agg_start_rid = rand_num(m_num_of_Racks);
            m_free_racks.clear();
            m_free_racks.shrink_to_fit();
            if (m_agg_start_rid + agg_racks_num >= m_num_of_Racks)
            {
              for (int i = (m_agg_start_rid + agg_racks_num) % m_num_of_Racks; i < m_agg_start_rid; i++)
              {
                m_free_racks.push_back(i);
              }
            }
            else
            {
              for (int i = 0; i < m_agg_start_rid; i++)
              {
                m_free_racks.push_back(i);
              }
              for (int i = m_agg_start_rid + agg_racks_num; i < m_num_of_Racks; i++)
              {
                m_free_racks.push_back(i);
              }
            }
            std::vector<int> temp;
            temp.push_back(stripe_id);
            m_merge_groups.push_back(temp);
          }
          else
          {
            m_merge_groups[idx].push_back(stripe_id);
          }

          int agg_rack_id = m_agg_start_rid - 1;
          int t_rack_id = -1;
          int g_rack_id = (m_agg_start_rid + agg_racks_num - 1) % m_num_of_Racks;
          for (int i = 0; i < l; i++)
          {
            for (int j = i * b; j < (i + 1) * b; j += g_m + 1)
            {
              bool flag = false;
              if (j + g_m + 1 >= (i + 1) * b)
                flag = true;
              if (flag && j + g_m + 1 != (i + 1) * b)
              {
                agg_rack_id++;
                if (agg_rack_id >= m_num_of_Racks)
                {
                  agg_rack_id = agg_rack_id % m_num_of_Racks;
                }
                t_rack_id = agg_rack_id;
              }
              else
              {
                t_rack_id = m_free_racks[rand_num(int(m_free_racks.size()))];
                auto iter = std::find(m_free_racks.begin(), m_free_racks.end(), t_rack_id);
                if (iter != m_free_racks.end())
                {
                  m_free_racks.erase(iter);
                }
              }
              Rack &t_rack = m_rack_table[t_rack_id];
              // place every g+1 data blocks from each group to a single rack
              for (int o = j; o < j + g_m + 1 && o < (i + 1) * b; o++)
              {
                // randomly select a node in the selected rack
                int t_node_id = randomly_select_a_node_in_rack(t_rack_id, stripe_id);
                blocks_info[o].map2rack = t_rack_id;
                blocks_info[o].map2node = t_node_id;
                update_stripe_info_in_node(true, t_node_id, stripe_id);
                t_rack.blocks.push_back(&blocks_info[o]);
                t_rack.stripes.insert(stripe_id);
                stripe_info.place2racks.insert(t_rack_id);
              }
              // place local parity blocks
              if (flag)
              {
                if (j + g_m + 1 != (i + 1) * b) // b % (g + 1) != 0
                {
                  // randomly select a node in the selected rack
                  int t_node_id = randomly_select_a_node_in_rack(t_rack_id, stripe_id);
                  blocks_info[k + g_m + i].map2rack = t_rack_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  t_rack.blocks.push_back(&blocks_info[k + g_m + i]);
                  t_rack.stripes.insert(stripe_id);
                  stripe_info.place2racks.insert(t_rack_id);
                }
                else // place the local parity blocks together with global ones
                {
                  Rack &g_rack = m_rack_table[g_rack_id];
                  // randomly select a node in the selected rack
                  int t_node_id = randomly_select_a_node_in_rack(g_rack_id, stripe_id);
                  blocks_info[k + g_m + i].map2rack = g_rack_id;
                  blocks_info[k + g_m + i].map2node = t_node_id;
                  update_stripe_info_in_node(true, t_node_id, stripe_id);
                  g_rack.blocks.push_back(&blocks_info[k + g_m + i]);
                  g_rack.stripes.insert(stripe_id);
                  stripe_info.place2racks.insert(g_rack_id);
                }
              }
            }
          }
          Rack &g_rack = m_rack_table[g_rack_id];
          // place the global parity blocks to the selected rack
          for (int i = 0; i < g_m; i++)
          {
            // randomly select a node in the selected rack
            int t_node_id = randomly_select_a_node_in_rack(g_rack_id, stripe_id);
            blocks_info[k + i].map2rack = g_rack_id;
            blocks_info[k + i].map2node = t_node_id;
            update_stripe_info_in_node(true, t_node_id, stripe_id);
            g_rack.blocks.push_back(&blocks_info[k + i]);
            g_rack.stripes.insert(stripe_id);
            stripe_info.place2racks.insert(g_rack_id);
          }
        }
      }
    }

    if (IF_DEBUG)
    {
      std::cout << std::endl;
      std::cout << "Data placement result:" << std::endl;
      for (int j = 0; j < m_num_of_Racks; j++)
      {
        Rack &t_rack = m_rack_table[j];
        if (int(t_rack.blocks.size()) > 0)
        {
          std::cout << "Rack " << j << ": ";
          for (auto it = t_rack.blocks.begin(); it != t_rack.blocks.end(); it++)
          {
            std::cout << "[" << (*it)->block_key << ":S" << (*it)->map2stripe << "G" << (*it)->map2group << "N" << (*it)->map2node << "] ";
          }
          std::cout << std::endl;
        }
      }
      std::cout << std::endl;
      std::cout << "Merge Group: ";
      for (auto it1 = m_merge_groups.begin(); it1 != m_merge_groups.end(); it1++)
      {
        std::cout << "[ ";
        for (auto it2 = (*it1).begin(); it2 != (*it1).end(); it2++)
        {
          std::cout << (*it2) << " ";
        }
        std::cout << "] ";
      }
      std::cout << std::endl;
    }

    // randomly select a rack
    int r_idx = rand_num(int(stripe_info.place2racks.size()));
    int s_rack_id = *(std::next(stripe_info.place2racks.begin(), r_idx));
    if (IF_DEBUG)
    {
      std::cout << "[SET] Select the proxy in Rack " << s_rack_id << " to encode and set!" << std::endl;
    }
    return s_rack_id;
  }

  int CoordinatorImpl::generate_placement_for_hpc(int stripe_id, int block_size)
  {
    Stripe &stripe_info = m_stripe_table[stripe_id];
    int k1 = stripe_info.k1;
    int m1 = stripe_info.m1;
    int k2 = stripe_info.k2;
    int m2 = stripe_info.m2;
    int x = m_encode_parameters.x_stripepermergegroup;
    ECProject::SingleStripePlacementType s_placement_type = m_encode_parameters.s_stripe_placementtype;
    ECProject::MultiStripesPlacementType m_placement_type = m_encode_parameters.m_stripe_placementtype;

    // generate stripe information
    int index = stripe_info.object_keys.size() - 1;
    std::string object_key = stripe_info.object_keys[index];
    Block *blocks_info = new Block[(k1 + m1) * (k2 + m2)];
    std::vector<Block *> block_ptrs((k1 + m1) * (k2 + m2));
    for (int i = 0; i < (k1 + m1) * (k2 + m2); i++)
    {
      blocks_info[i].block_id = i;
      blocks_info[i].block_size = block_size;
      blocks_info[i].map2stripe = stripe_id;
      blocks_info[i].map2key = object_key;
      if (i < k1 * k2)
      {
        std::string tmp = "_D";
        if (i < 10)
          tmp = "_D0";
        blocks_info[i].block_key = object_key + tmp + std::to_string(i);
        blocks_info[i].block_type = 'D';
        blocks_info[i].map2row = i / k1;
        blocks_info[i].map2col = i % k1;
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
      else if (i >= k1 * k2 && i < (k1 + m1) * k2)
      {
        int tmp_id = i - k1 * k2;
        blocks_info[i].block_key = "Stripe" + std::to_string(stripe_id) + "_R" + std::to_string(tmp_id);
        blocks_info[i].block_type = 'R';
        blocks_info[i].map2row = tmp_id / m1;
        blocks_info[i].map2col = tmp_id % m1 + k1;
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
      else if (i >= (k1 + m1) * k2 && i < (k1 + m1) * k2 + k1 * m2)
      {
        int tmp_id = i - (k1 + m1) * k2;
        blocks_info[i].block_key = "Stripe" + std::to_string(stripe_id) + "_C" + std::to_string(tmp_id);
        blocks_info[i].block_type = 'C';
        blocks_info[i].map2row = tmp_id / k1 + k2;
        blocks_info[i].map2col = tmp_id % k1;
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
      else
      {
        int tmp_id = i - (k1 + m1) * k2 - k1 * m2;
        blocks_info[i].block_key = "Stripe" + std::to_string(stripe_id) + "_G" + std::to_string(tmp_id);
        blocks_info[i].block_type = 'G';
        blocks_info[i].map2row = tmp_id / m1 + k2;
        blocks_info[i].map2col = tmp_id % m1 + k1;
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
      int index = blocks_info[i].map2col * (k2 + m2) + blocks_info[i].map2row;
      block_ptrs[index] = &blocks_info[i];
    }

    int index_in_EHPC = stripe_id % x;
    if (index_in_EHPC == 0)
    {
      std::vector<int> new_group;
      new_group.push_back(stripe_id);
      m_merge_groups.push_back(new_group);
      m_col2rack.clear();
    }
    else
    {
      int index = int(m_merge_groups.size()) - 1;
      m_merge_groups[index].push_back(stripe_id);
    }

    int col_num_for_each = m1;
    int required_rack_num = ceil(x * k1, m1) + 1;
    if (m_placement_type == Vertical)
      required_rack_num = ceil(k1, m1) + 1;
    if (m_col2rack.empty())
    {
      int c_index = 0;
      // randomly selected n racks
      std::vector<int> selected_racks;
      rand_n_num(0, m_num_of_Racks - 1, required_rack_num, selected_racks);
      for (int i = 0; i < required_rack_num; i++)
      {
        int t_rack_id = selected_racks[i];
        for (int j = 0; j < col_num_for_each; j++)
        {
          m_col2rack[c_index++] = t_rack_id;
        }
        if (IF_DEBUG)
        {
          std::cout << t_rack_id << " ";
        }
      }
      if (IF_DEBUG)
        std::cout << "Randomly select " << required_rack_num << " racks from totally " << m_num_of_Racks << " racks." << std::endl;
    }

    // data placement
    if (s_placement_type == Optimal)
    {
      if (m_placement_type == Horizontal)
      {
        for (int i = 0; i < k1 + m1; i++)
        {
          int real_index = index_in_EHPC * k1 + i;
          if (i >= k1)
          {
            real_index = x * k1 + i % k1;
          }
          int t_rack_id = m_col2rack[real_index];
          for (int j = 0; j < k2 + m2; j++)
          {
            Rack &t_rack = m_rack_table[t_rack_id];
            int t_node_id = randomly_select_a_node_in_rack(t_rack_id, stripe_id);
            block_ptrs[i * (k2 + m2) + j]->map2rack = t_rack_id;
            block_ptrs[i * (k2 + m2) + j]->map2node = t_node_id;
            update_stripe_info_in_node(true, t_node_id, stripe_id);
            t_rack.blocks.push_back(block_ptrs[i * (k2 + m2) + j]);
            m_rack_table[t_rack_id].stripes.insert(stripe_id);
            stripe_info.place2racks.insert(t_rack_id);
          }
        }
      }
      else if (m_placement_type == Vertical)
      {
        for (int i = 0; i < k1 + m1; i++)
        {
          int t_rack_id = m_col2rack[i];
          for (int j = 0; j < k2 + m2; j++)
          {
            Rack &t_rack = m_rack_table[t_rack_id];
            int t_node_id = randomly_select_a_node_in_rack(t_rack_id, stripe_id);
            block_ptrs[i * (k2 + m2) + j]->map2rack = t_rack_id;
            block_ptrs[i * (k2 + m2) + j]->map2node = t_node_id;
            update_stripe_info_in_node(true, t_node_id, stripe_id);
            t_rack.blocks.push_back(block_ptrs[i * (k2 + m2) + j]);
            m_rack_table[t_rack_id].stripes.insert(stripe_id);
            stripe_info.place2racks.insert(t_rack_id);
          }
        }
      }
    }

    if (IF_DEBUG)
    {
      std::cout << std::endl;
      std::cout << "Data placement result:" << std::endl;
      for (int j = 0; j < m_num_of_Racks; j++)
      {
        Rack &t_rack = m_rack_table[j];
        if (int(t_rack.blocks.size()) > 0)
        {
          std::cout << "Rack " << j << ": ";
          for (auto it = t_rack.blocks.begin(); it != t_rack.blocks.end(); it++)
          {
            std::cout << "[" << (*it)->block_key << ":S" << (*it)->map2stripe << "R" << (*it)->map2row << "C" << (*it)->map2col << "N" << (*it)->map2node << "] ";
          }
          std::cout << std::endl;
        }
      }
      std::cout << std::endl;
      std::cout << "Merge Group: ";
      for (auto it1 = m_merge_groups.begin(); it1 != m_merge_groups.end(); it1++)
      {
        std::cout << "[ ";
        for (auto it2 = (*it1).begin(); it2 != (*it1).end(); it2++)
        {
          std::cout << (*it2) << " ";
        }
        std::cout << "] ";
      }
      std::cout << std::endl;
    }

    // randomly select a proxy
    int selected_rack_id = m_col2rack[rand_num(k1)];
    if (m_placement_type == Horizontal)
    {
      selected_rack_id = m_col2rack[index_in_EHPC * k1 + rand_num(k1)];
    }
    if (IF_DEBUG)
    {
      std::cout << "[SET] Select the proxy in Rack " << selected_rack_id << " to encode and set!" << std::endl;
    }
    return selected_rack_id;
  }

  int CoordinatorImpl::generate_placement_for_mlec(int stripe_id, int block_size)
  {
    Stripe &stripe_info = m_stripe_table[stripe_id];
    int k1 = stripe_info.k1;
    int m1 = stripe_info.m1;
    int k2 = stripe_info.k2;
    int m2 = stripe_info.m2;
    int x = m_encode_parameters.x_stripepermergegroup;
    ECProject::SingleStripePlacementType s_placement_type = m_encode_parameters.s_stripe_placementtype;
    ECProject::MultiStripesPlacementType m_placement_type = m_encode_parameters.m_stripe_placementtype;

    // generate stripe information
    int index = stripe_info.object_keys.size() - 1;
    std::string object_key = stripe_info.object_keys[index];
    Block *blocks_info = new Block[(k1 + m1) * (k2 + m2)];
    std::vector<Block *> block_ptrs((k1 + m1) * (k2 + m2));
    for (int i = 0; i < (k1 + m1) * (k2 + m2); i++)
    {
      blocks_info[i].block_id = i;
      blocks_info[i].block_size = block_size;
      blocks_info[i].map2stripe = stripe_id;
      blocks_info[i].map2key = object_key;
      if (i < k1 * k2)
      {
        std::string tmp = "_D";
        if (i < 10)
          tmp = "_D0";
        blocks_info[i].block_key = object_key + tmp + std::to_string(i);
        blocks_info[i].block_type = 'D';
        blocks_info[i].map2row = i / k1;
        blocks_info[i].map2col = i % k1;
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
      else if (i >= k1 * k2 && i < (k1 + m1) * k2)
      {
        int tmp_id = i - k1 * k2;
        blocks_info[i].block_key = "Stripe" + std::to_string(stripe_id) + "_R" + std::to_string(tmp_id);
        blocks_info[i].block_type = 'R';
        blocks_info[i].map2row = tmp_id / m1;
        blocks_info[i].map2col = tmp_id % m1 + k1;
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
      else if (i >= (k1 + m1) * k2 && i < (k1 + m1) * k2 + k1 * m2)
      {
        int tmp_id = i - (k1 + m1) * k2;
        blocks_info[i].block_key = "Stripe" + std::to_string(stripe_id) + "_C" + std::to_string(tmp_id);
        blocks_info[i].block_type = 'C';
        blocks_info[i].map2row = tmp_id / k1 + k2;
        blocks_info[i].map2col = tmp_id % k1;
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
      else
      {
        int tmp_id = i - (k1 + m1) * k2 - k1 * m2;
        blocks_info[i].block_key = "Stripe" + std::to_string(stripe_id) + "_G" + std::to_string(tmp_id);
        blocks_info[i].block_type = 'G';
        blocks_info[i].map2row = tmp_id / m1 + k2;
        blocks_info[i].map2col = tmp_id % m1 + k1;
        stripe_info.blocks.push_back(&blocks_info[i]);
      }
      int index = blocks_info[i].map2col * (k2 + m2) + blocks_info[i].map2row;
      block_ptrs[index] = &blocks_info[i];
    }
    int index_in_EHPC = stripe_id % x;
    if (index_in_EHPC == 0)
    {
      std::vector<int> new_group;
      new_group.push_back(stripe_id);
      m_merge_groups.push_back(new_group);
      m_m1cols2rack.clear();
    }
    else
    {
      int index = int(m_merge_groups.size()) - 1;
      m_merge_groups[index].push_back(stripe_id);
    }
    int required_rack_num = x * ceil(k1, m1) + 1;
    if (m_placement_type == Vertical)
      required_rack_num = ceil(k1, m1) + 1;
    if (m_m1cols2rack.empty())
    {
      int c_index = 0;
      // randomly selected n racks
      std::vector<int> selected_racks;
      rand_n_num(0, m_num_of_Racks - 1, required_rack_num, selected_racks);
      for (int i = 0; i < required_rack_num; i++)
      {
        m_m1cols2rack[c_index++] = selected_racks[i];
        if (IF_DEBUG)
        {
          std::cout << selected_racks[i] << " ";
        }
      }
      if (IF_DEBUG)
        std::cout << std::endl;
    }

    // data placement
    if (s_placement_type == Optimal)
    {
      // for every m1 data columns
      for (int i = 0; i < k1; i++)
      {
        int real_index = i / m1;
        if (m_placement_type == Horizontal)
        {
          real_index = index_in_EHPC * ceil(k1, m1) + i / m1;
        }
        int t_rack_id = m_m1cols2rack[real_index];
        // for every column
        for (int j = 0; j < k2 + m2; j++)
        {
          int t_node_id = randomly_select_a_node_in_rack(t_rack_id, stripe_id);
          block_ptrs[i * (k2 + m2) + j]->map2rack = t_rack_id;
          block_ptrs[i * (k2 + m2) + j]->map2node = t_node_id;
          update_stripe_info_in_node(true, t_node_id, stripe_id);
          Rack &t_rack = m_rack_table[t_rack_id];
          t_rack.blocks.push_back(block_ptrs[i * (k2 + m2) + j]);
          m_rack_table[t_rack_id].stripes.insert(stripe_id);
          stripe_info.place2racks.insert(t_rack_id);
        }
      }
      int c_idx = (int)m_m1cols2rack.size() - 1;
      int t_rack_id = m_m1cols2rack[c_idx];
      // for the m1 parity columns
      for (int i = k1; i < k1 + m1; i++)
      {
        for (int j = 0; j < k2 + m2; j++)
        {
          int t_node_id = randomly_select_a_node_in_rack(t_rack_id, stripe_id);
          block_ptrs[i * (k2 + m2) + j]->map2rack = t_rack_id;
          block_ptrs[i * (k2 + m2) + j]->map2node = t_node_id;
          update_stripe_info_in_node(true, t_node_id, stripe_id);
          Rack &t_rack = m_rack_table[t_rack_id];
          t_rack.blocks.push_back(block_ptrs[i * (k2 + m2) + j]);
          m_rack_table[t_rack_id].stripes.insert(stripe_id);
          stripe_info.place2racks.insert(t_rack_id);
        }
      }
    }

    if (IF_DEBUG)
    {
      std::cout << std::endl;
      std::cout << "Data placement result:" << std::endl;
      for (int j = 0; j < m_num_of_Racks; j++)
      {
        Rack &t_rack = m_rack_table[j];
        if (int(t_rack.blocks.size()) > 0)
        {
          std::cout << "Rack " << j << ": ";
          for (auto it = t_rack.blocks.begin(); it != t_rack.blocks.end(); it++)
          {
            std::cout << "[" << (*it)->block_key << ":S" << (*it)->map2stripe << "R" << (*it)->map2row << "C" << (*it)->map2col << "N" << (*it)->map2node << "] ";
          }
          std::cout << std::endl;
        }
      }
      std::cout << std::endl;
      std::cout << "Merge Group: ";
      for (auto it1 = m_merge_groups.begin(); it1 != m_merge_groups.end(); it1++)
      {
        std::cout << "[ ";
        for (auto it2 = (*it1).begin(); it2 != (*it1).end(); it2++)
        {
          std::cout << (*it2) << " ";
        }
        std::cout << "] ";
      }
      std::cout << std::endl;
    }

    // select a proxy
    int idx = rand_num((k1 + m1) * (k2 + m2));
    Stripe &t_stripe = m_stripe_table[stripe_id];
    int selected_rack_id = t_stripe.blocks[idx]->map2rack;
    if (IF_DEBUG)
    {
      std::cout << "[SET] Select the proxy in Rack " << selected_rack_id << " to encode and set!" << std::endl;
    }
    return selected_rack_id;
  }
}