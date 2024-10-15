#include "coordinator.h"
#include "tinyxml2.h"
#include <random>
#include <algorithm>
#include <unistd.h>
#include <sys/time.h>

namespace ECProject
{
  bool CoordinatorImpl::init_proxyinfo()
  {
    for (auto cur = m_rack_table.begin(); cur != m_rack_table.end(); cur++)
    {
      std::string proxy_ip_and_port = cur->second.proxy_ip + ":" + std::to_string(cur->second.proxy_port);
      auto _stub = proxy_proto::proxyService::NewStub(grpc::CreateChannel(proxy_ip_and_port, grpc::InsecureChannelCredentials()));
      proxy_proto::CheckaliveCMD Cmd;
      proxy_proto::RequestResult result;
      grpc::ClientContext clientContext;
      Cmd.set_name("coordinator");
      grpc::Status status;
      status = _stub->checkalive(&clientContext, Cmd, &result);
      if (status.ok())
      {
        std::cout << "[Proxy Check] ok from " << proxy_ip_and_port << std::endl;
      }
      else
      {
        std::cout << "[Proxy Check] failed to connect " << proxy_ip_and_port << std::endl;
      }
      m_proxy_ptrs.insert(std::make_pair(proxy_ip_and_port, std::move(_stub)));
    }
    return true;
  }

  bool CoordinatorImpl::init_clusterinfo(std::string m_clusterinfo_path)
  {
    std::cout << "Cluster_information_path:" << m_clusterinfo_path << std::endl;
    tinyxml2::XMLDocument xml;
    xml.LoadFile(m_clusterinfo_path.c_str());
    tinyxml2::XMLElement *root = xml.RootElement();
    int node_id = 0;
    m_num_of_Racks = 0;
    for (tinyxml2::XMLElement *rack = root->FirstChildElement(); rack != nullptr; rack = rack->NextSiblingElement())
    {
      std::string rack_id(rack->Attribute("id"));
      Rack t_rack;
      m_rack_table[std::stoi(rack_id)] = t_rack;
      m_rack_table[std::stoi(rack_id)].rack_id = std::stoi(rack_id);
      std::string proxy(rack->Attribute("proxy"));
      std::cout << "rack_id: " << rack_id << " , proxy: " << proxy << std::endl;
      auto pos = proxy.find(':');
      m_rack_table[std::stoi(rack_id)].proxy_ip = proxy.substr(0, pos);
      m_rack_table[std::stoi(rack_id)].proxy_port = std::stoi(proxy.substr(pos + 1, proxy.size()));
      int node_cnt = 0;
      for (tinyxml2::XMLElement *node = rack->FirstChildElement()->FirstChildElement(); node != nullptr; node = node->NextSiblingElement())
      {
        std::string node_uri(node->Attribute("uri"));
        std::cout << "   |__node: " << node_uri << std::endl;
        m_rack_table[std::stoi(rack_id)].nodes.push_back(node_id);
        m_node_table[node_id].node_id = node_id;
        auto pos = node_uri.find(':');
        m_node_table[node_id].node_ip = node_uri.substr(0, pos);
        m_node_table[node_id].node_port = std::stoi(node_uri.substr(pos + 1, node_uri.size()));
        m_node_table[node_id].map2rack = std::stoi(rack_id);
        node_id++;
        node_cnt++;
      }
      m_num_of_Nodes_in_Rack = node_cnt;
      m_num_of_Racks++;
    }
    return true;
  }

  int CoordinatorImpl::randomly_select_a_rack(int stripe_id)
  {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis_rack(0, m_num_of_Racks - 1);
    int r_rack_id = dis_rack(gen);
    while (m_rack_table[r_rack_id].stripes.find(stripe_id) != m_rack_table[r_rack_id].stripes.end())
    {
      r_rack_id = dis_rack(gen);
    }
    return r_rack_id;
  }

  int CoordinatorImpl::randomly_select_a_node_in_rack(int rack_id, int stripe_id)
  {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis_node(0, m_rack_table[rack_id].nodes.size() - 1);
    int r_node_id = m_rack_table[rack_id].nodes[dis_node(gen)];
    while (m_node_table[r_node_id].stripes.find(stripe_id) != m_node_table[r_node_id].stripes.end())
    {
      r_node_id = m_rack_table[rack_id].nodes[dis_node(gen)];
    }
    return r_node_id;
  }

  void CoordinatorImpl::update_stripe_info_in_node(bool add_or_sub, int t_node_id, int stripe_id)
  {
    int stripe_block_num = 0;
    if (m_node_table[t_node_id].stripes.find(stripe_id) != m_node_table[t_node_id].stripes.end())
    {
      stripe_block_num = m_node_table[t_node_id].stripes[stripe_id];
    }
    if (add_or_sub)
    {
      m_node_table[t_node_id].stripes[stripe_id] = stripe_block_num + 1;
    }
    else
    {
      if (stripe_block_num == 1)
      {
        m_node_table[t_node_id].stripes.erase(stripe_id);
      }
      else
      {
        m_node_table[t_node_id].stripes[stripe_id] = stripe_block_num - 1;
      }
    }
  }

  void CoordinatorImpl::update_tables_when_rm_stripe(int stripe_id)
  {
    m_stripe_table.erase(stripe_id);
    for (auto it = m_node_table.begin(); it != m_node_table.end(); it++)
    {
      Node &t_node = it->second;
      auto it_ = t_node.stripes.find(stripe_id);
      if (it_ != t_node.stripes.end())
      {
        t_node.stripes.erase(stripe_id);
      }
    }
    for (auto it = m_rack_table.begin(); it != m_rack_table.end(); it++)
    {
      Rack &t_rack = it->second;
      auto it_ = t_rack.stripes.find(stripe_id);
      if (it_ != t_rack.stripes.end())
      {
        t_rack.stripes.erase(stripe_id);
      }
    }
  }

  void CoordinatorImpl::blocks_in_rack(std::map<char, std::vector<ECProject::Block *>> &block_info, int rack_id, int stripe_id)
  {
    std::vector<ECProject::Block *> tt, td, tl, tg;
    Rack &rack = m_rack_table[rack_id];
    for (auto it = rack.blocks.begin(); it != rack.blocks.end(); it++)
    {
      Block *block = *it;
      if (block->map2stripe == stripe_id)
      {
        tt.push_back(block);
        if (block->block_type == 'D')
        {
          td.push_back(block);
        }
        else if (block->block_type == 'L')
        {
          tl.push_back(block);
        }
        else
        {
          tg.push_back(block);
        }
      }
    }
    block_info['T'] = tt;
    block_info['D'] = td;
    block_info['L'] = tl;
    block_info['G'] = tg;
  }

  void CoordinatorImpl::find_max_group(int &max_group_id, int &max_group_num, int rack_id, int stripe_id)
  {
    int group_cnt[20] = {0};
    Rack &rack = m_rack_table[rack_id];
    for (auto it = rack.blocks.begin(); it != rack.blocks.end(); it++)
    {
      if ((*it)->map2stripe == stripe_id)
      {
        group_cnt[(*it)->map2group]++;
      }
    }

    for (int i = 0; i < 20; i++)
    {
      if (group_cnt[i] > max_group_num)
      {
        max_group_id = i;
        max_group_num = group_cnt[i];
      }
    }
  }

  int CoordinatorImpl::count_block_num(char type, int rack_id, int stripe_id, int group_id)
  {
    int cnt = 0;
    Rack &rack = m_rack_table[rack_id];
    for (auto it = rack.blocks.begin(); it != rack.blocks.end(); it++)
    {
      Block *block = *it;
      if (block->map2stripe == stripe_id)
      {
        if (group_id == -1)
        {
          if (type == 'T')
          {
            cnt++;
          }
          else if (block->block_type == type)
          {
            cnt++;
          }
        }
        else if (int(block->map2group) == group_id)
        {
          if (type == 'T')
          {
            cnt++;
          }
          else if (block->block_type == type)
          {
            cnt++;
          }
        }
      }
    }
    if (cnt == 0)
    {
      rack.stripes.erase(stripe_id);
    }
    return cnt;
  }

  // find out if any specific type of block from the stripe exists in the rack
  bool CoordinatorImpl::find_block(char type, int rack_id, int stripe_id)
  {
    Rack &rack = m_rack_table[rack_id];
    for (auto it = rack.blocks.begin(); it != rack.blocks.end(); it++)
    {
      if (stripe_id != -1 && int((*it)->map2stripe) == stripe_id && (*it)->block_type == type)
      {
        return true;
      }
      else if (stripe_id == -1 && (*it)->block_type == type)
      {
        return true;
      }
    }
    return false;
  }

  void CoordinatorImpl::block_num_in_rack(std::map<int, int> &rack_blocks, int stripe_id)
  {
    Stripe &t_stripe = m_stripe_table[stripe_id];
    for (auto it = t_stripe.place2racks.begin(); it != t_stripe.place2racks.end(); it++)
    {
      int t_rack_id = *it;
      Rack &t_rack = m_rack_table[t_rack_id];
      int cnt = 0;
      for (int j = 0; j < int(t_rack.blocks.size()); j++)
      {
        if (t_rack.blocks[j]->map2stripe == stripe_id)
        {
          cnt++;
        }
      }
      rack_blocks[t_rack_id] = cnt;
    }
  }
}