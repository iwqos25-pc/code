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

template <typename T>
inline std::vector<size_t> argsort(const std::vector<T> &v)
{
  std::vector<size_t> idx(v.size());
  std::iota(idx.begin(), idx.end(), 0);
  std::sort(idx.begin(), idx.end(), [&v](size_t i1, size_t i2)
            { return v[i1] < v[i2]; });
  return idx;
};

namespace ECProject
{
  void CoordinatorImpl::request_merge_rs(coordinator_proto::RepIfMerged *mergeReplyClient)
  {
    int x = m_encode_parameters.x_stripepermergegroup;
    int tot_stripe_num = int(m_stripe_table.size());
    int stripe_cnt = 0;
    std::vector<std::vector<int>> new_merge_groups;
    double t_rc = 0.0, o_rc = 0.0;
    // for simulation
    int t_cross_rack = 0;

    // for each merge group, every x stripes a merge group
    for (auto it_m = m_merge_groups.begin(); it_m != m_merge_groups.end(); it_m++)
    {
      std::vector<int> s_merge_group;
      // for each x stripes
      for (auto it_s = (*it_m).begin(); it_s != (*it_m).end(); it_s += x)
      {
        gettimeofday(&start_time, NULL);
        Stripe &tmp_stripe = m_stripe_table[(*it_s)];
        int k = tmp_stripe.k;
        int m = tmp_stripe.g_m;
        int l_stripe_id = m_cur_stripe_id;
        int p_rack_id = -1;
        std::vector<int> p_node_id;
        std::unordered_set<int> old_stripe_id_set;

        // for request
        std::map<int, proxy_proto::locationInfo> block_location;
        proxy_proto::mainRecalPlan p_main_plan;
        proxy_proto::NodeAndBlock old_parities;
        std::unordered_set<int> old_parities_rack_set;

        int block_size;
        // merge and generate new stripe information
        Stripe larger_stripe;
        larger_stripe.stripe_id = l_stripe_id;
        larger_stripe.k = k * x;
        larger_stripe.g_m = m;
        larger_stripe.l = tmp_stripe.l * x;
        int seri_num = 0;
        // for each stripe
        for (auto it_t = it_s; it_t != it_s + x; it_t++, seri_num++)
        {
          int t_stripe_id = *(it_t);
          old_stripe_id_set.insert(t_stripe_id);
          Stripe &t_stripe = m_stripe_table[t_stripe_id];
          larger_stripe.object_keys.insert(larger_stripe.object_keys.end(), t_stripe.object_keys.begin(), t_stripe.object_keys.end());
          // for each block
          for (auto it_b = t_stripe.blocks.begin(); it_b != t_stripe.blocks.end(); it_b++)
          {
            Block *t_block = *it_b;
            block_size = t_block->block_size;
            update_stripe_info_in_node(false, t_block->map2node, t_block->map2stripe);
            m_rack_table[t_block->map2rack].stripes.erase(t_block->map2stripe);
            t_block->map2stripe = l_stripe_id;
            if (t_block->block_type == 'D') // data blocks
            {
              int t_rack_id = t_block->map2rack;
              t_block->block_id = seri_num * k + t_block->block_id;
              t_block->map2group = t_block->block_id / m;
              t_block->map2stripe = l_stripe_id;
              update_stripe_info_in_node(true, t_block->map2node, l_stripe_id);
              m_rack_table[t_block->map2rack].stripes.insert(l_stripe_id);
              larger_stripe.blocks.push_back(t_block);
              // for parity block recalculation, find out the location of each data block
              if (block_location.find(t_rack_id) == block_location.end())
              {
                int t_rack_id = t_block->map2rack;
                Rack &t_rack = m_rack_table[t_rack_id];
                proxy_proto::locationInfo new_location;
                new_location.set_rack_id(t_rack_id);
                new_location.set_proxy_ip(t_rack.proxy_ip);
                new_location.set_proxy_port(t_rack.proxy_port);
                block_location[t_rack_id] = new_location;
              }
              int t_node_id = t_block->map2node;
              Node &t_node = m_node_table[t_node_id];
              proxy_proto::locationInfo &t_location = block_location[t_rack_id];
              t_location.add_datanodeip(t_node.node_ip);
              t_location.add_datanodeport(t_node.node_port);
              t_location.add_blockkeys(t_block->block_key);
              t_location.add_blockids(t_block->block_id);
            }
            else // old parity blocks
            {
              p_rack_id = t_block->map2rack;
              Node &p_node = m_node_table[t_block->map2node];
              p_node_id.push_back(t_block->map2node);
              // remove the old parity blocks from the rack
              Rack &p_rack = m_rack_table[p_rack_id];
              for (auto it_c = p_rack.blocks.begin(); it_c != p_rack.blocks.end(); it_c++)
              {
                if ((*it_c)->block_key == t_block->block_key)
                {
                  p_rack.blocks.erase(it_c);
                  break;
                }
              }
              // for delete
              old_parities.add_datanodeip(p_node.node_ip);
              old_parities.add_datanodeport(p_node.node_port);
              old_parities.add_blockkeys(t_block->block_key);
              old_parities_rack_set.insert(t_block->map2rack);
            }
          }
          larger_stripe.place2racks.insert(t_stripe.place2racks.begin(), t_stripe.place2racks.end());
        }
        if (IF_DEBUG)
        {
          std::cout << "p_rack_id : " << p_rack_id << std::endl;
          std::cout << "p_node_id : ";
          for (int i = 0; i < int(p_node_id.size()); i++)
          {
            std::cout << p_node_id[i] << " ";
          }
          std::cout << std::endl;
        }
        if (IF_DEBUG)
        {
          std::cout << "\033[1;33m[MERGE] Select rack and node to place new global parity blocks:\033[0m" << std::endl;
        }

        // generate new parity block
        for (int i = 0; i < m; i++)
        {
          std::string t_block_key = "Stripe" + std::to_string(l_stripe_id) + "_P" + std::to_string(i);
          int t_map2node = p_node_id[m * (x - 1) + i];
          int t_block_id = larger_stripe.k + i;
          int t_map2rack = m_node_table[t_map2node].map2rack;
          Block *t_block = new Block(t_block_id, t_block_key, 'P', block_size, larger_stripe.l, l_stripe_id, t_map2rack, t_map2node, "");
          if (IF_DEBUG)
          {
            std::cout << "\033[1;33m" << t_block->block_key << ": rack" << t_block->map2rack << ", Node" << t_block->map2node << "\033[0m" << std::endl;
          }
          larger_stripe.blocks.push_back(t_block);
          update_stripe_info_in_node(true, t_map2node, l_stripe_id);
          m_rack_table[p_rack_id].stripes.insert(l_stripe_id);
          Rack &t_rack = m_rack_table[t_map2rack];
          t_rack.blocks.push_back(t_block);
          auto it = std::find(t_rack.nodes.begin(), t_rack.nodes.end(), t_map2node);
          if (it == t_rack.nodes.end())
          {
            std::cout << "[Generate new parity block] the selected node not in the selected rack!" << std::endl;
          }
          // for global parity block recalculation, the location of the new parities
          Node &p_node = m_node_table[t_map2node];
          p_main_plan.add_p_datanodeip(p_node.node_ip);
          p_main_plan.add_p_datanodeport(p_node.node_port);
          p_main_plan.add_p_blockkeys(t_block_key);
        }

        // parity block recalculation
        p_main_plan.set_type(true);
        p_main_plan.set_k(larger_stripe.k);
        p_main_plan.set_l(larger_stripe.l);
        p_main_plan.set_g_m(m);
        p_main_plan.set_block_size(block_size);
        p_main_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
        p_main_plan.set_stripe_id(l_stripe_id);
        p_main_plan.set_encodetype(m_encode_parameters.encodetype);
        for (auto itb = block_location.begin(); itb != block_location.end(); itb++)
        {
          proxy_proto::locationInfo t_location = block_location[itb->first];
          auto new_rack = p_main_plan.add_racks();
          new_rack->set_rack_id(t_location.rack_id());
          new_rack->set_proxy_ip(t_location.proxy_ip());
          new_rack->set_proxy_port(t_location.proxy_port());
          for (int ii = 0; ii < int(t_location.blockkeys_size()); ii++)
          {
            new_rack->add_datanodeip(t_location.datanodeip(ii));
            new_rack->add_datanodeport(t_location.datanodeport(ii));
            new_rack->add_blockkeys(t_location.blockkeys(ii));
            new_rack->add_blockids(t_location.blockids(ii));
          }
        }
        gettimeofday(&end_time, NULL);
        m_meta_access_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

        // for simulation
        simulation_recalculate(p_main_plan, p_rack_id, t_cross_rack);

        auto send_main_plan = [this, p_main_plan, p_rack_id]() mutable
        {
          // main
          grpc::ClientContext context_m;
          proxy_proto::RecalReply response_m;
          std::string chosen_proxy_m = m_rack_table[p_rack_id].proxy_ip + ":" + std::to_string(m_rack_table[p_rack_id].proxy_port);
          grpc::Status stat1 = m_proxy_ptrs[chosen_proxy_m]->mainRecal(&context_m, p_main_plan, &response_m);
          if (IF_DEBUG)
          {
            std::cout << "Selected main proxy " << chosen_proxy_m << std::endl;
          }
        };

        // help
        auto send_help_plan = [this, larger_stripe, block_location, p_rack_id, block_size, m](int first)
        {
          proxy_proto::helpRecalPlan p_help_plan;
          proxy_proto::locationInfo t_location = block_location.at(first);
          p_help_plan.set_k(larger_stripe.k);
          p_help_plan.set_type(true);
          p_help_plan.set_encodetype(m_encode_parameters.encodetype);
          p_help_plan.set_mainproxyip(m_rack_table[p_rack_id].proxy_ip);
          // port to accept data: mainproxy_port + rack_id + 2
          p_help_plan.set_mainproxyport(m_rack_table[p_rack_id].proxy_port + 1);
          for (int ii = 0; ii < int(t_location.blockkeys_size()); ii++)
          {
            p_help_plan.add_datanodeip(t_location.datanodeip(ii));
            p_help_plan.add_datanodeport(t_location.datanodeport(ii));
            p_help_plan.add_blockkeys(t_location.blockkeys(ii));
            p_help_plan.add_blockids(t_location.blockids(ii));
          }
          p_help_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
          p_help_plan.set_block_size(block_size);
          p_help_plan.set_parity_num(m);
          grpc::ClientContext context_h;
          proxy_proto::RecalReply response_h;
          std::string chosen_proxy_h = t_location.proxy_ip() + ":" + std::to_string(t_location.proxy_port());
          grpc::Status stat = m_proxy_ptrs[chosen_proxy_h]->helpRecal(&context_h, p_help_plan, &response_h);
          if (IF_DEBUG)
          {
            std::cout << "Selected helper proxy " << chosen_proxy_h << std::endl;
          }
        };

        double temp_time = 0.0;
        gettimeofday(&start_time, NULL);
        try
        {
          if (IF_DEBUG)
          {
            std::cout << "[Parities Recalculation] Send main and help proxy plans!" << std::endl;
          }
          std::thread my_main_thread(send_main_plan);
          std::vector<std::thread> senders;
          for (auto itb = block_location.begin(); itb != block_location.end(); itb++)
          {
            if (itb->first != p_rack_id)
            {
              senders.push_back(std::thread(send_help_plan, itb->first));
            }
          }
          for (int j = 0; j < int(senders.size()); j++)
          {
            senders[j].join();
          }
          my_main_thread.join();
        }
        catch (const std::exception &e)
        {
          std::cerr << e.what() << '\n';
        }
        // check
        proxy_proto::AskIfSuccess ask_c0;
        ask_c0.set_step(0);
        grpc::ClientContext context_c0;
        proxy_proto::RepIfSuccess response_c0;
        std::string chosen_proxy_c0 = m_rack_table[p_rack_id].proxy_ip + ":" + std::to_string(m_rack_table[p_rack_id].proxy_port);
        grpc::Status stat_c0 = m_proxy_ptrs[chosen_proxy_c0]->checkStep(&context_c0, ask_c0, &response_c0);
        if (stat_c0.ok() && response_c0.ifsuccess() && IF_DEBUG)
        {
          std::cout << "[MERGE] global parity block recalculate success for Stripe" << l_stripe_id << std::endl;
        }
        m_cross_rack_time += response_c0.cross_rack_time();
        m_inner_rack_time += response_c0.inner_rack_time();
        m_encoding_time += response_c0.encoding_time();

        // send delete old parity blocks request
        grpc::ClientContext del_context;
        proxy_proto::DelReply del_reply;
        old_parities.set_stripe_id(-1);
        old_parities.set_key("");
        // randomly select a proxy
        int idx = rand_num(int(old_parities_rack_set.size()));
        int del_rack_id = *(std::next(old_parities_rack_set.begin(), idx));
        std::string del_chosen_proxy = m_rack_table[del_rack_id].proxy_ip + ":" + std::to_string(m_rack_table[del_rack_id].proxy_port);
        grpc::Status del_status = m_proxy_ptrs[del_chosen_proxy]->deleteBlock(&del_context, old_parities, &del_reply);
        if (del_status.ok() && del_reply.ifcommit())
        {
          std::cout << "[MERGE] Delete old parity blocks success!" << std::endl;
        }
        gettimeofday(&end_time, NULL);
        temp_time = end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
        o_rc = t_rc;
        t_rc += temp_time;

        gettimeofday(&start_time, NULL);
        // update stripes meta information
        std::unordered_set<int>::iterator iter;
        for (iter = old_stripe_id_set.begin(); iter != old_stripe_id_set.end(); iter++)
        {
          auto its = m_stripe_table.find(*iter);
          if (its != m_stripe_table.end())
          {
            m_stripe_table.erase(its);
          }
        }
        m_stripe_table[l_stripe_id] = larger_stripe;
        m_cur_stripe_id++;
        s_merge_group.push_back(l_stripe_id);
        stripe_cnt += x;
        gettimeofday(&end_time, NULL);
        m_meta_access_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

        std::cout << "[Merging Stage " << m_merge_degree + 1 << "] Process " << stripe_cnt << "/" << tot_stripe_num
                  << " time:" << t_rc - o_rc << std::endl;
        if (stripe_cnt == tot_stripe_num)
        {
          std::cout << "[Merging Average Cost]" << " time:" << (t_rc) / (tot_stripe_num / x) << std::endl;
        }
      }
      new_merge_groups.push_back(s_merge_group);
    }
    m_merge_groups.clear();
    m_merge_groups = new_merge_groups;
    m_merge_degree += 1;
    mergeReplyClient->set_ifmerged(true);
    mergeReplyClient->set_gc(t_rc);
    mergeReplyClient->set_dc(0);
    mergeReplyClient->set_cross_rack_num(t_cross_rack);
    mergeReplyClient->set_cross_rack_time(m_cross_rack_time);
    mergeReplyClient->set_inner_rack_time(m_inner_rack_time);
    mergeReplyClient->set_encoding_time(m_encoding_time);
    mergeReplyClient->set_meta_time(m_meta_access_time);
    m_cross_rack_time = 0;
    m_inner_rack_time = 0;
    m_encoding_time = 0;
    m_meta_access_time = 0;

    if (IF_DEBUG)
    {
      // print the result
      std::cout << std::endl;
      std::cout << "After Merging:" << std::endl;
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
      std::cout << std::endl;
    }
  }

  void CoordinatorImpl::request_merge_lrc(int l, int b, int g_m, int m, int num_of_stripes, coordinator_proto::RepIfMerged *mergeReplyClient)
  {
    // EncodeType encodetype = m_encode_parameters.encodetype;
    int tot_stripe_num = int(m_stripe_table.size());
    int stripe_cnt = 0;
    // double t_lc = 0.0;
    double o_gc = 0.0;
    double o_dc = 0.0;
    double t_gc = 0.0;
    double t_dc = 0.0;
    // for simulation
    int t_cross_rack = 0;
    std::vector<std::vector<int>>::iterator it_g;
    std::vector<int>::iterator it_s, it_t;
    std::vector<std::vector<int>> new_merge_groups;
    for (it_g = m_merge_groups.begin(); it_g != m_merge_groups.end(); it_g++)
    {
      std::vector<int> s_merge_group;
      // for each xi stripes
      for (it_s = (*it_g).begin(); it_s != (*it_g).end(); it_s += num_of_stripes)
      {
        gettimeofday(&start_time, NULL);
        int s_stripe_id = *(it_s);
        Stripe &s_stripe = m_stripe_table[s_stripe_id];
        int s_k = s_stripe.k;
        int s_l = s_stripe.l;

        int cur_block_id = 0;
        int cur_l_block_id = s_k * num_of_stripes + m_encode_parameters.g_m_globalparityblock;
        int cur_group_id = 0;
        int l_stripe_id = m_cur_stripe_id;
        int g_rack_id;
        std::vector<int> g_node_id;
        std::unordered_set<int> old_stripe_id_set;

        // for request
        std::map<int, proxy_proto::locationInfo> block_location;
        proxy_proto::mainRecalPlan g_main_plan;
        proxy_proto::NodeAndBlock old_parities;
        std::unordered_set<int> old_parities_rack_set;

        int block_size;
        // merge and generate new stripe information
        Stripe larger_stripe;
        larger_stripe.stripe_id = l_stripe_id;
        larger_stripe.k = s_k * num_of_stripes;
        larger_stripe.l = s_l * num_of_stripes;
        larger_stripe.g_m = m_encode_parameters.g_m_globalparityblock;
        // for each stripe
        for (it_t = it_s; it_t != it_s + num_of_stripes; it_t++)
        {
          int t_stripe_id = *(it_t);
          old_stripe_id_set.insert(t_stripe_id);
          Stripe &t_stripe = m_stripe_table[t_stripe_id];

          larger_stripe.object_keys.insert(larger_stripe.object_keys.end(), t_stripe.object_keys.begin(), t_stripe.object_keys.end());
          // larger_stripe.object_sizes.insert(larger_stripe.object_sizes.end(), t_stripe.object_sizes.begin(), t_stripe.object_sizes.end());
          std::vector<Block *>::iterator it_b, it_c;
          // for each block
          for (it_b = t_stripe.blocks.begin(); it_b != t_stripe.blocks.end(); it_b++)
          {
            Block *t_block = *it_b;
            // update the stripe info in node/rack
            update_stripe_info_in_node(false, t_block->map2node, t_block->map2stripe);
            m_rack_table[t_block->map2rack].stripes.erase(t_block->map2stripe);
            if (t_block->block_type == 'D')
            {
              int t_rack_id = t_block->map2rack;
              t_block->block_id = cur_block_id++;
              t_block->map2group = t_block->block_id / b;
              t_block->map2stripe = l_stripe_id;
              update_stripe_info_in_node(true, t_block->map2node, l_stripe_id);
              m_rack_table[t_block->map2rack].stripes.insert(l_stripe_id);
              larger_stripe.blocks.push_back(t_block);

              // for global parity block recalculation, find out the location of each data block
              if (block_location.find(t_rack_id) == block_location.end())
              {
                int t_rack_id = t_block->map2rack;
                Rack &t_rack = m_rack_table[t_rack_id];
                proxy_proto::locationInfo new_location;
                new_location.set_rack_id(t_rack_id);
                new_location.set_proxy_ip(t_rack.proxy_ip);
                new_location.set_proxy_port(t_rack.proxy_port);
                block_location[t_rack_id] = new_location;
              }
              int t_node_id = t_block->map2node;
              Node &t_node = m_node_table[t_node_id];
              proxy_proto::locationInfo &t_location = block_location[t_rack_id];
              t_location.add_datanodeip(t_node.node_ip);
              t_location.add_datanodeport(t_node.node_port);
              t_location.add_blockkeys(t_block->block_key);
              t_location.add_blockids(t_block->block_id);
            }
            else if (t_block->block_type == 'L') // for local parities
            {
              t_block->block_id = cur_l_block_id++;
              t_block->map2group = cur_group_id++;
              t_block->map2stripe = l_stripe_id;
              update_stripe_info_in_node(true, t_block->map2node, l_stripe_id);
              m_rack_table[t_block->map2rack].stripes.insert(l_stripe_id);
              larger_stripe.blocks.push_back(t_block);
            }
            else if (t_block->block_type == 'G')
            {
              // for global parity block recalculation
              g_rack_id = t_block->map2rack;
              Node &g_node = m_node_table[t_block->map2node];
              g_node_id.push_back(t_block->map2node);
              // remove the old global parity block from the rack
              Rack &g_rack = m_rack_table[g_rack_id];
              for (it_c = g_rack.blocks.begin(); it_c != g_rack.blocks.end(); it_c++)
              {
                if ((*it_c)->block_key == t_block->block_key)
                {
                  g_rack.blocks.erase(it_c);
                  break;
                }
              }
              // for delete
              old_parities.add_datanodeip(g_node.node_ip);
              old_parities.add_datanodeport(g_node.node_port);
              old_parities.add_blockkeys(t_block->block_key);
              old_parities_rack_set.insert(t_block->map2rack);
            }
            block_size = t_block->block_size;
          }
          larger_stripe.place2racks.insert(t_stripe.place2racks.begin(), t_stripe.place2racks.end());
        }
        if (IF_DEBUG)
        {
          std::cout << "g_rack_id : " << g_rack_id << std::endl;
          std::cout << "g_node_id : ";
          for (int i = 0; i < int(g_node_id.size()); i++)
          {
            std::cout << g_node_id[i] << " ";
          }
          std::cout << std::endl;
        }
        if (IF_DEBUG)
        {
          std::cout << "\033[1;33m[MERGE] Select rack and node to place new global parity blocks:\033[0m" << std::endl;
        }
        // generate new parity block
        for (int i = 0; i < g_m; i++)
        {
          std::string t_block_key = "Stripe" + std::to_string(l_stripe_id) + "_G" + std::to_string(i);
          int t_map2node = g_node_id[g_m * (num_of_stripes - 1) + i];
          int t_block_id = larger_stripe.k + i;
          int t_map2rack = m_node_table[t_map2node].map2rack;
          Block *t_block = new Block(t_block_id, t_block_key, 'G', block_size, larger_stripe.l, l_stripe_id, t_map2rack, t_map2node, "");
          if (IF_DEBUG)
          {
            std::cout << "\033[1;33m" << t_block->block_key << ": rack" << t_block->map2rack << ", Node" << t_block->map2node << "\033[0m" << std::endl;
          }
          larger_stripe.blocks.push_back(t_block);
          update_stripe_info_in_node(true, t_map2node, l_stripe_id);
          m_rack_table[g_rack_id].stripes.insert(l_stripe_id);
          Rack &t_rack = m_rack_table[t_map2rack];
          t_rack.blocks.push_back(t_block);
          auto it = std::find(t_rack.nodes.begin(), t_rack.nodes.end(), t_map2node);
          if (it == t_rack.nodes.end())
          {
            std::cout << "[Generate new parity block] the selected node not in the selected rack!" << std::endl;
          }
          // for global parity block recalculation, the location of the new parities
          Node &g_node = m_node_table[t_map2node];
          g_main_plan.add_p_datanodeip(g_node.node_ip);
          g_main_plan.add_p_datanodeport(g_node.node_port);
          g_main_plan.add_p_blockkeys(t_block_key);
        }

        if (IF_DEBUG)
        {
          // print the result
          std::cout << std::endl;
          std::cout << "Data placement before data block relocation:" << std::endl;
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
        }

        // find out the data blocks to relocate
        int num2mov_v = 0, num2mov_k = 0;
        std::unordered_set<int>::iterator it;
        std::vector<std::string> block_to_move_key;
        std::vector<int> block_src_node;
        std::vector<int> block_des_node;

        // what about keeping all blocks in the fewest racks?
        // to keep each local group placed in fewest rack
        int bi = larger_stripe.k / larger_stripe.l;
        int c_a = ceil(bi + 1, g_m + 1);
        for (int i = 0; i < larger_stripe.l; i++) // for each local group
        {
          int c_b = 0;
          std::vector<int> block_from_group_in_rack;
          // std::cout << "\033[1;31m";
          for (int j = 0; j < m_num_of_Racks; j++)
          {
            int b_cnt = count_block_num('T', j, l_stripe_id, i);
            if (b_cnt > 0)
              c_b++;
            block_from_group_in_rack.push_back(b_cnt);
            // std::cout << j << ":" << b_cnt << " ";
          }
          // std::cout << "\033[0m" << std::endl;
          if (c_b > c_a)
          {
            if (IF_DEBUG)
            {
              std::cout << "\033[1;31m[MERGE] Group " << i << " rack number: actual-" << c_b << " expected-" << c_a << "\033[0m" << std::endl;
            }
            int c_m = c_b - c_a;
            auto idxs = argsort(block_from_group_in_rack);
            std::vector<int> del_rack;
            int c_cnt = 0;
            int idx = 0;
            while (c_cnt < c_m)
            {
              if (block_from_group_in_rack[idxs[idx]] > 0)
              {
                del_rack.push_back(idxs[idx]);
                c_cnt++;
              }
              idx++;
            }
            std::vector<ECProject::Block *> block_to_move;
            std::vector<ECProject::Block *>::iterator it_b, it_c;
            for (int j = 0; j < int(del_rack.size()); j++)
            {
              int t_rack_id = del_rack[j];
              Rack &t_rack = m_rack_table[t_rack_id];
              for (it_b = t_rack.blocks.begin(); it_b != t_rack.blocks.end(); it_b++)
              {
                if ((*it_b)->map2stripe == l_stripe_id && int((*it_b)->map2group) == i)
                {
                  block_to_move.push_back((*it_b));
                }
              }
            }
            // find destination rack and node for each moved block
            for (it_b = block_to_move.begin(); it_b != block_to_move.end(); it_b++)
            {
              int t_rack_id = (*it_b)->map2rack;
              block_to_move_key.push_back((*it_b)->block_key);
              block_src_node.push_back((*it_b)->map2node);
              bool flag_m = false;
              std::unordered_set<int>::iterator it_a;
              for (it_a = larger_stripe.place2racks.begin(); it_a != larger_stripe.place2racks.end(); it_a++)
              {
                int d_rack_id = *it_a;
                if (d_rack_id != t_rack_id)
                {
                  int max_group_id = -1, max_group_num = 0;
                  int block_num = count_block_num('T', d_rack_id, l_stripe_id, -1);
                  find_max_group(max_group_id, max_group_num, d_rack_id, l_stripe_id);
                  if (!find_block('G', d_rack_id, l_stripe_id) && block_num > 0 && block_num < g_m + 1 && (*it_b)->map2group == max_group_id)
                  {
                    update_stripe_info_in_node(false, (*it_b)->map2node, (*it_b)->map2stripe);
                    int r_node_id = randomly_select_a_node_in_rack(d_rack_id, l_stripe_id);
                    (*it_b)->map2rack = d_rack_id;
                    (*it_b)->map2node = r_node_id;
                    Rack &s_rack = m_rack_table[t_rack_id];
                    for (it_c = s_rack.blocks.begin(); it_c != s_rack.blocks.end(); it_c++)
                    {
                      if ((*it_c)->block_key == (*it_b)->block_key)
                      {
                        s_rack.blocks.erase(it_c);
                        break;
                      }
                    }
                    Rack &d_rack = m_rack_table[d_rack_id];
                    d_rack.blocks.push_back((*it_b));
                    block_des_node.push_back((*it_b)->map2node);
                    update_stripe_info_in_node(true, (*it_b)->map2node, l_stripe_id);
                    m_rack_table[(*it_b)->map2rack].stripes.insert(l_stripe_id);
                    flag_m = true;
                    break;
                  }
                }
              }
              if (!flag_m)
              {
                std::cout << "[MERGE] reloc2 : can't find out a des-rack to move block " << (*it_b)->block_key << std::endl;
              }
            }
          }
        }
        if (IF_DEBUG)
        {
          std::cout << "[MERGE] all blocks to relocate:";
          for (int ii = 0; ii < int(block_to_move_key.size()); ii++)
          {
            std::cout << block_to_move_key[ii] << "[" << block_src_node[ii] << "->" << block_des_node[ii] << "] ";
          }
          std::cout << std::endl;
        }
        num2mov_k = int(block_to_move_key.size()) - num2mov_v;

        // remove the 'empty' rack from the set
        // std::vector<int> empty_racks;
        std::unordered_set<int>::iterator it_a;
        if (IF_DEBUG)
        {
          std::cout << "[MERGE] Racks that places Stripe " << l_stripe_id << ":";
        }
        for (it_a = larger_stripe.place2racks.begin(); it_a != larger_stripe.place2racks.end();)
        {
          int t_cid = *it_a;
          if (IF_DEBUG)
          {
            std::cout << " " << t_cid;
          }
          if (t_cid >= 0 && count_block_num('T', t_cid, l_stripe_id, -1) == 0)
          {
            // empty_racks.push_back(t_cid);
            it_a = larger_stripe.place2racks.erase(it_a);
            if (IF_DEBUG)
            {
              std::cout << "(remove)";
            }
          }
          else
          {
            it_a++;
          }
        }
        if (IF_DEBUG)
        {
          std::cout << std::endl;
        }
        gettimeofday(&end_time, NULL);
        m_meta_access_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

        // time
        double temp_time = 0.0;
        struct timeval g_start_time, g_end_time;
        struct timeval d_start_time, d_end_time;

        if (IF_DEBUG)
        {
          std::cout << "[MERGE] Start to recalculate global parity blocks for Stripe" << l_stripe_id << std::endl;
        }

        // global parity block recalculation
        g_main_plan.set_type(true);
        g_main_plan.set_k(larger_stripe.k);
        g_main_plan.set_l(larger_stripe.l);
        g_main_plan.set_g_m(g_m);
        g_main_plan.set_block_size(block_size);
        g_main_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
        g_main_plan.set_stripe_id(l_stripe_id);
        g_main_plan.set_encodetype(m_encode_parameters.encodetype);
        for (auto itb = block_location.begin(); itb != block_location.end(); itb++)
        {
          proxy_proto::locationInfo t_location = block_location[itb->first];
          auto new_rack = g_main_plan.add_racks();
          new_rack->set_rack_id(t_location.rack_id());
          new_rack->set_proxy_ip(t_location.proxy_ip());
          new_rack->set_proxy_port(t_location.proxy_port());
          for (int ii = 0; ii < int(t_location.blockkeys_size()); ii++)
          {
            new_rack->add_datanodeip(t_location.datanodeip(ii));
            new_rack->add_datanodeport(t_location.datanodeport(ii));
            new_rack->add_blockkeys(t_location.blockkeys(ii));
            new_rack->add_blockids(t_location.blockids(ii));
          }
        }

        // for simulation
        simulation_recalculate(g_main_plan, g_rack_id, t_cross_rack);

        auto send_main_plan = [this, g_main_plan, g_rack_id]() mutable
        {
          // main
          grpc::ClientContext context_m;
          proxy_proto::RecalReply response_m;
          std::string chosen_proxy_m = m_rack_table[g_rack_id].proxy_ip + ":" + std::to_string(m_rack_table[g_rack_id].proxy_port);
          grpc::Status stat1 = m_proxy_ptrs[chosen_proxy_m]->mainRecal(&context_m, g_main_plan, &response_m);
          if (IF_DEBUG)
          {
            std::cout << "Selected main proxy " << chosen_proxy_m << std::endl;
          }
        };

        // help
        auto send_help_plan = [this, larger_stripe, block_location, g_rack_id, block_size, g_m](int first)
        {
          proxy_proto::helpRecalPlan g_help_plan;
          proxy_proto::locationInfo t_location = block_location.at(first);
          g_help_plan.set_k(larger_stripe.k);
          g_help_plan.set_type(true);
          g_help_plan.set_encodetype(m_encode_parameters.encodetype);
          g_help_plan.set_mainproxyip(m_rack_table[g_rack_id].proxy_ip);
          // port to accept data: mainproxy_port + rack_id + 2
          g_help_plan.set_mainproxyport(m_rack_table[g_rack_id].proxy_port + 1);
          for (int ii = 0; ii < int(t_location.blockkeys_size()); ii++)
          {
            g_help_plan.add_datanodeip(t_location.datanodeip(ii));
            g_help_plan.add_datanodeport(t_location.datanodeport(ii));
            g_help_plan.add_blockkeys(t_location.blockkeys(ii));
            g_help_plan.add_blockids(t_location.blockids(ii));
          }
          g_help_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
          g_help_plan.set_block_size(block_size);
          g_help_plan.set_parity_num(g_m);
          grpc::ClientContext context_h;
          proxy_proto::RecalReply response_h;
          std::string chosen_proxy_h = t_location.proxy_ip() + ":" + std::to_string(t_location.proxy_port());
          grpc::Status stat = m_proxy_ptrs[chosen_proxy_h]->helpRecal(&context_h, g_help_plan, &response_h);
          if (IF_DEBUG)
          {
            std::cout << "Selected helper proxy " << chosen_proxy_h << std::endl;
          }
        };

        temp_time = 0.0;
        gettimeofday(&g_start_time, NULL);
        try
        {
          if (IF_DEBUG)
          {
            std::cout << "[Global Parities Recalculation] Send main and help proxy plans!" << std::endl;
          }
          std::thread my_main_thread(send_main_plan);
          std::vector<std::thread> senders;
          for (auto itb = block_location.begin(); itb != block_location.end(); itb++)
          {
            if (itb->first != g_rack_id)
            {
              // send_help_plan(itb->first);
              senders.push_back(std::thread(send_help_plan, itb->first));
            }
          }
          for (int j = 0; j < int(senders.size()); j++)
          {
            senders[j].join();
          }
          my_main_thread.join();
        }
        catch (const std::exception &e)
        {
          std::cerr << e.what() << '\n';
        }
        // check
        proxy_proto::AskIfSuccess ask_c0;
        ask_c0.set_step(0);
        grpc::ClientContext context_c0;
        proxy_proto::RepIfSuccess response_c0;
        std::string chosen_proxy_c0 = m_rack_table[g_rack_id].proxy_ip + ":" + std::to_string(m_rack_table[g_rack_id].proxy_port);
        grpc::Status stat_c0 = m_proxy_ptrs[chosen_proxy_c0]->checkStep(&context_c0, ask_c0, &response_c0);
        if (stat_c0.ok() && response_c0.ifsuccess() && IF_DEBUG)
        {
          std::cout << "[MERGE] global parity block recalculate success for Stripe" << l_stripe_id << std::endl;
        }
        m_cross_rack_time += response_c0.cross_rack_time();
        m_inner_rack_time += response_c0.inner_rack_time();
        m_encoding_time += response_c0.encoding_time();

        // send delete old parity blocks request
        grpc::ClientContext del_context;
        proxy_proto::DelReply del_reply;
        old_parities.set_stripe_id(-1);
        old_parities.set_key("");
        // randomly select a proxy
        int idx = rand_num(int(old_parities_rack_set.size()));
        int del_rack_id = *(std::next(old_parities_rack_set.begin(), idx));
        std::string del_chosen_proxy = m_rack_table[del_rack_id].proxy_ip + ":" + std::to_string(m_rack_table[del_rack_id].proxy_port);
        grpc::Status del_status = m_proxy_ptrs[del_chosen_proxy]->deleteBlock(&del_context, old_parities, &del_reply);
        if (del_status.ok() && del_reply.ifcommit())
        {
          std::cout << "[MERGE] Delete old parity blocks success!" << std::endl;
        }
        gettimeofday(&g_end_time, NULL);
        temp_time = g_end_time.tv_sec - g_start_time.tv_sec + (g_end_time.tv_usec - g_start_time.tv_usec) * 1.0 / 1000000;
        o_gc = t_gc;
        t_gc += temp_time;

        // data block relocation
        if (int(block_to_move_key.size()) > 0)
        {
          if (IF_DEBUG)
          {
            std::cout << "[MERGE] Start to relocate data blocks for Stripe" << l_stripe_id << std::endl;
          }
          temp_time = 0.0;
          gettimeofday(&d_start_time, NULL);
          proxy_proto::blockRelocPlan b_reloc_plan;
          for (int i = 0; i < int(block_to_move_key.size()); i++)
          {
            int src_node_id = block_src_node[i];
            int des_node_id = block_des_node[i];
            b_reloc_plan.add_blocktomove(block_to_move_key[i]);
            b_reloc_plan.add_fromdatanodeip(m_node_table[src_node_id].node_ip);
            b_reloc_plan.add_fromdatanodeport(m_node_table[src_node_id].node_port);
            b_reloc_plan.add_todatanodeip(m_node_table[des_node_id].node_ip);
            b_reloc_plan.add_todatanodeport(m_node_table[des_node_id].node_port);
            // for simulation
            int src_rack_id = m_node_table[src_node_id].map2rack;
            int des_rack_id = m_node_table[des_node_id].map2rack;
            if (src_rack_id != des_rack_id)
            {
              t_cross_rack++;
            }
          }
          b_reloc_plan.set_block_size(block_size);
          int r_rack_id = rand_num(m_num_of_Racks);
          std::string chosen_proxy_b = m_rack_table[r_rack_id].proxy_ip + ":" + std::to_string(m_rack_table[r_rack_id].proxy_port);
          grpc::ClientContext context_b;
          proxy_proto::blockRelocReply response_b;
          grpc::Status stat_b = m_proxy_ptrs[chosen_proxy_b]->blockReloc(&context_b, b_reloc_plan, &response_b);
          // check
          proxy_proto::AskIfSuccess ask_c2;
          ask_c2.set_step(2);
          grpc::ClientContext context_c2;
          proxy_proto::RepIfSuccess response_c2;
          grpc::Status stat_c2 = m_proxy_ptrs[chosen_proxy_b]->checkStep(&context_c2, ask_c2, &response_c2);
          if (stat_c2.ok() && response_c2.ifsuccess() && IF_DEBUG)
          {
            std::cout << "[MERGE] block relocaltion success!" << std::endl;
          }
          gettimeofday(&d_end_time, NULL);
          temp_time = d_end_time.tv_sec - d_start_time.tv_sec + (d_end_time.tv_usec - d_start_time.tv_usec) * 1.0 / 1000000;
          o_dc = t_dc;
          t_dc += temp_time;
          m_cross_rack_time += response_c2.cross_rack_time();
          m_inner_rack_time += response_c2.inner_rack_time();
        }

        gettimeofday(&start_time, NULL);
        // update stripes meta information
        std::unordered_set<int>::iterator iter;
        for (iter = old_stripe_id_set.begin(); iter != old_stripe_id_set.end(); iter++)
        {
          auto its = m_stripe_table.find(*iter);
          if (its != m_stripe_table.end())
          {
            m_stripe_table.erase(its);
          }
        }
        m_stripe_table[l_stripe_id] = larger_stripe;
        m_cur_stripe_id++;
        s_merge_group.push_back(l_stripe_id);
        stripe_cnt += num_of_stripes;
        gettimeofday(&end_time, NULL);
        m_meta_access_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

        std::cout << "[Merging Stage " << m_merge_degree + 1 << "] Process " << stripe_cnt << "/" << tot_stripe_num
                  << " gc:" << t_gc - o_gc << " dc:" << t_dc - o_dc << " total:" << t_gc - o_gc + t_dc - o_dc << std::endl;
        if (stripe_cnt == tot_stripe_num)
        {
          std::cout << "[Merging Average Cost]" << " gc:" << t_gc / (tot_stripe_num / num_of_stripes) << " dc:" << t_dc / (tot_stripe_num / num_of_stripes)
                    << " total:" << (t_gc + t_dc) / (tot_stripe_num / num_of_stripes) << std::endl;
        }
      }
      new_merge_groups.push_back(s_merge_group);
    }
    // update m_merge_groups
    m_merge_groups.clear();
    m_merge_groups = new_merge_groups;
    mergeReplyClient->set_ifmerged(true);
    mergeReplyClient->set_gc(t_gc);
    mergeReplyClient->set_dc(t_dc);
    mergeReplyClient->set_cross_rack_num(t_cross_rack);
    mergeReplyClient->set_cross_rack_time(m_cross_rack_time);
    mergeReplyClient->set_inner_rack_time(m_inner_rack_time);
    mergeReplyClient->set_encoding_time(m_encoding_time);
    mergeReplyClient->set_meta_time(m_meta_access_time);
    m_cross_rack_time = 0;
    m_inner_rack_time = 0;
    m_encoding_time = 0;
    m_meta_access_time = 0;

    if (IF_DEBUG)
    {
      // print the result
      std::cout << std::endl;
      std::cout << "After Merging:" << std::endl;
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
      std::cout << std::endl;
    }
    m_merge_degree += 1;
  }

  void CoordinatorImpl::request_merge_hpc(coordinator_proto::RepIfMerged *mergeReplyClient)
  {
    int x = m_encode_parameters.x_stripepermergegroup;

    int tot_stripe_num = int(m_stripe_table.size());
    int stripe_cnt = 0;
    std::vector<std::vector<int>> new_merge_groups;
    double t_rc = 0.0;
    double t_mc = 0.0;
    // for simulation
    int t_cross_rack = 0;

    // for each merge group, every x stripes a merge group
    for (auto it_m = m_merge_groups.begin(); it_m != m_merge_groups.end(); it_m++)
    {
      std::vector<int> s_merge_group;
      // for each x stripes
      for (auto it_s = (*it_m).begin(); it_s != (*it_m).end(); it_s += x)
      {
        gettimeofday(&start_time, NULL);
        Stripe &tmp_stripe = m_stripe_table[(*it_s)];
        int k1 = tmp_stripe.k1;
        int m1 = tmp_stripe.m1;
        int k2 = tmp_stripe.k2;
        int m2 = tmp_stripe.m2;
        int racks_num = 1;
        bool isvertical = false;
        if (m_encode_parameters.m_stripe_placementtype == ECProject::Vertical)
        {
          racks_num = (k1 + m1) / m1;
          isvertical = true;
        }
        int l_stripe_id = m_cur_stripe_id;
        // to figure out the nodes to place the new parities
        int r_node_ids[k2 * m1];
        int c_node_ids[k1 * m2];
        int g_node_ids[m1 * m2];
        std::unordered_set<int> old_stripe_id_set;

        // for request
        std::map<int, proxy_proto::MergePlanHPC> merge_plan;
        proxy_proto::NodeAndBlock old_parities;
        std::unordered_set<int> old_parities_rack_set;

        // merge and generate new stripe information
        int block_size;
        Stripe larger_stripe;
        if (isvertical)
        {
          larger_stripe.k1 = k1;
          larger_stripe.k2 = x * k2;
        }
        else
        {
          larger_stripe.k2 = k2;
          larger_stripe.k1 = x * k1;
        }
        larger_stripe.m1 = m1;
        larger_stripe.m2 = m2;
        larger_stripe.stripe_id = l_stripe_id;
        int seri_num = 0;
        for (auto it_t = it_s; it_t != it_s + x; it_t++, seri_num++)
        {
          int t_stripe_id = (*it_t);
          old_stripe_id_set.insert(t_stripe_id);
          Stripe &t_stripe = m_stripe_table[t_stripe_id];
          larger_stripe.object_keys.insert(larger_stripe.object_keys.end(), t_stripe.object_keys.begin(), t_stripe.object_keys.end());
          // larger_stripe.object_sizes.insert(larger_stripe.object_sizes.end(), t_stripe.object_sizes.begin(), t_stripe.object_sizes.end());
          for (auto it_b = t_stripe.blocks.begin(); it_b != t_stripe.blocks.end(); it_b++)
          {
            Block *t_block = *it_b;
            block_size = t_block->block_size;
            update_stripe_info_in_node(false, t_block->map2node, t_block->map2stripe);
            m_rack_table[t_block->map2rack].stripes.erase(t_block->map2stripe);
            t_block->map2stripe = l_stripe_id;
            if (t_block->block_type == 'D')
            {
              if (isvertical)
              {
                t_block->map2row = t_block->map2row + seri_num * k2;
                t_block->block_id = t_block->map2row * k1 + t_block->map2col;
              }
              else
              {
                t_block->map2col = t_block->map2col + seri_num * k1;
                t_block->block_id = t_block->map2row * k1 * x + t_block->map2col;
              }
              larger_stripe.blocks.push_back(t_block);
              update_stripe_info_in_node(true, t_block->map2node, l_stripe_id);
              m_rack_table[t_block->map2rack].stripes.insert(l_stripe_id);
            }
            else if (t_block->block_type == 'R')
            {
              if (isvertical)
              {
                t_block->map2row = t_block->map2row + seri_num * k2;
                t_block->block_id = x * k1 * k2 + t_block->map2row * m1 + t_block->map2col - k1;
                larger_stripe.blocks.push_back(t_block);
                update_stripe_info_in_node(true, t_block->map2node, l_stripe_id);
                m_rack_table[t_block->map2rack].stripes.insert(l_stripe_id);
              }
              else
              {
                int r_rack_id = t_block->map2rack;
                int r_map2idx = t_block->map2row * m1 + t_block->map2col - k1;
                Node &r_node = m_node_table[t_block->map2node];
                r_node_ids[r_map2idx] = t_block->map2node;
                if (merge_plan.find(r_rack_id) == merge_plan.end())
                {
                  proxy_proto::MergePlanHPC new_plan;
                  new_plan.add_datanodeip(r_node.node_ip);
                  new_plan.add_datanodeport(r_node.node_port);
                  new_plan.add_blockkeys(t_block->block_key);
                  new_plan.add_blockidxs(r_map2idx);
                  merge_plan[r_rack_id] = new_plan;
                }
                else
                {
                  proxy_proto::MergePlanHPC &t_plan = merge_plan[r_rack_id];
                  t_plan.add_datanodeip(r_node.node_ip);
                  t_plan.add_datanodeport(r_node.node_port);
                  t_plan.add_blockkeys(t_block->block_key);
                  t_plan.add_blockidxs(r_map2idx);
                }
                // remove the old parities from the rack
                Rack &r_rack = m_rack_table[r_rack_id];
                for (auto it_r = r_rack.blocks.begin(); it_r != r_rack.blocks.end(); it_r++)
                {
                  if ((*it_r)->block_key == t_block->block_key)
                  {
                    r_rack.blocks.erase(it_r);
                    break;
                  }
                }
                // for delete
                old_parities.add_datanodeip(r_node.node_ip);
                old_parities.add_datanodeport(r_node.node_port);
                old_parities.add_blockkeys(t_block->block_key);
                old_parities_rack_set.insert(t_block->map2rack);
              }
            }
            else if (t_block->block_type == 'C')
            {
              if (isvertical)
              {
                int c_rack_id = t_block->map2rack;
                int c_map2idx = (t_block->map2row - k2) * k1 + t_block->map2col;
                Node &c_node = m_node_table[t_block->map2node];
                c_node_ids[c_map2idx] = t_block->map2node;
                if (merge_plan.find(c_rack_id) == merge_plan.end())
                {
                  proxy_proto::MergePlanHPC new_plan;
                  new_plan.add_datanodeip(c_node.node_ip);
                  new_plan.add_datanodeport(c_node.node_port);
                  new_plan.add_blockkeys(t_block->block_key);
                  new_plan.add_blockidxs(c_map2idx);
                  merge_plan[c_rack_id] = new_plan;
                }
                else
                {
                  proxy_proto::MergePlanHPC &t_plan = merge_plan[c_rack_id];
                  t_plan.add_datanodeip(c_node.node_ip);
                  t_plan.add_datanodeport(c_node.node_port);
                  t_plan.add_blockkeys(t_block->block_key);
                  t_plan.add_blockidxs(c_map2idx);
                }
                // remove the old parities from the rack
                Rack &c_rack = m_rack_table[c_rack_id];
                for (auto it_c = c_rack.blocks.begin(); it_c != c_rack.blocks.end(); it_c++)
                {
                  if ((*it_c)->block_key == t_block->block_key)
                  {
                    c_rack.blocks.erase(it_c);
                    break;
                  }
                }
                // for delete
                old_parities.add_datanodeip(c_node.node_ip);
                old_parities.add_datanodeport(c_node.node_port);
                old_parities.add_blockkeys(t_block->block_key);
                old_parities_rack_set.insert(t_block->map2rack);
              }
              else
              {
                t_block->map2col = t_block->map2col + seri_num * k1;
                t_block->block_id = x * k1 * k2 + k2 * m1 + (t_block->map2row - k2) * x * k1 + t_block->map2col;
                larger_stripe.blocks.push_back(t_block);
                update_stripe_info_in_node(true, t_block->map2node, l_stripe_id);
                m_rack_table[t_block->map2rack].stripes.insert(l_stripe_id);
              }
            }
            else
            {
              int g_rack_id = t_block->map2rack;
              int g_map2idx = k2 * m1 + (t_block->map2row - k2) * m1 + t_block->map2col - k1;
              if (isvertical)
                g_map2idx = k1 * m2 + (t_block->map2row - k2) * m1 + t_block->map2col - k1;
              Node &g_node = m_node_table[t_block->map2node];
              g_node_ids[(t_block->map2row - k2) * m1 + t_block->map2col - k1] = t_block->map2node;
              if (merge_plan.find(g_rack_id) == merge_plan.end())
              {
                proxy_proto::MergePlanHPC new_plan;
                new_plan.add_datanodeip(g_node.node_ip);
                new_plan.add_datanodeport(g_node.node_port);
                new_plan.add_blockkeys(t_block->block_key);
                new_plan.add_blockidxs(g_map2idx);
                merge_plan[g_rack_id] = new_plan;
              }
              else
              {
                proxy_proto::MergePlanHPC &t_plan = merge_plan[g_rack_id];
                t_plan.add_datanodeip(g_node.node_ip);
                t_plan.add_datanodeport(g_node.node_port);
                t_plan.add_blockkeys(t_block->block_key);
                t_plan.add_blockidxs(g_map2idx);
              }
              // remove the old parities from the rack
              Rack &g_rack = m_rack_table[g_rack_id];
              for (auto it_g = g_rack.blocks.begin(); it_g != g_rack.blocks.end(); it_g++)
              {
                if ((*it_g)->block_key == t_block->block_key)
                {
                  g_rack.blocks.erase(it_g);
                  break;
                }
              }
              // for delete
              old_parities.add_datanodeip(g_node.node_ip);
              old_parities.add_datanodeport(g_node.node_port);
              old_parities.add_blockkeys(t_block->block_key);
              old_parities_rack_set.insert(t_block->map2rack);
            }
          }
          larger_stripe.place2racks.insert(t_stripe.place2racks.begin(), t_stripe.place2racks.end());
        }
        if (racks_num != int(merge_plan.size()) && IF_DEBUG)
        {
          std::cout << "Error! racks_num != plans_num. " << racks_num << " != " << int(merge_plan.size()) << std::endl;
        }
        // generate new parities
        if (isvertical)
        {
          for (int i = 0; i < k1 * m2 + m1 * m2; i++)
          {
            int t_map2row = x * k2 + i / k1;
            int t_map2col = i % k1;
            int t_block_id = x * k2 * (k1 + m1) + i;
            std::string t_block_key = "Stripe" + std::to_string(l_stripe_id) + "_C" + std::to_string(i);
            int t_map2node = c_node_ids[i];
            char t_block_type = 'C';
            if (i >= k1 * m2)
            {
              int j = i - k1 * m2;
              t_map2row = x * k2 + j / m1;
              t_map2col = k1 + j % m1;
              t_block_id = x * k2 * (k1 + m1) + k1 * m2 + j;
              t_block_key = "Stripe" + std::to_string(l_stripe_id) + "_G" + std::to_string(j);
              t_map2node = g_node_ids[j];
              t_block_type = 'G';
            }
            int t_map2rack = m_node_table[t_map2node].map2rack;
            Block *t_block = new Block(t_block_id, t_block_key, t_block_type, block_size, t_map2row, t_map2col,
                                       l_stripe_id, t_map2rack, t_map2node, "");
            if (IF_DEBUG)
            {
              std::cout << "\033[1;33m" << t_block->block_key << ": Rack" << t_block->map2rack << ", Node" << t_block->map2node << "\033[0m" << std::endl;
            }
            larger_stripe.blocks.push_back(t_block);
            update_stripe_info_in_node(true, t_map2node, l_stripe_id);
            m_rack_table[t_map2rack].stripes.insert(l_stripe_id);
            Rack &t_rack = m_rack_table[t_map2rack];
            t_rack.blocks.push_back(t_block);

            auto it = std::find(t_rack.nodes.begin(), t_rack.nodes.end(), t_map2node);
            if (it == t_rack.nodes.end())
            {
              std::cout << "[Generate new parity block] the selected node not in the selected rack!" << std::endl;
            }

            Node &t_node = m_node_table[t_map2node];
            proxy_proto::MergePlanHPC &t_plan = merge_plan[t_map2rack];
            t_plan.add_n_datanodeip(t_node.node_ip);
            t_plan.add_n_datanodeport(t_node.node_port);
            t_plan.add_n_blockkeys(t_block_key);
            t_plan.add_n_blockidxs(i);
          }
        }
        else
        {
          for (int i = 0; i < k2 * m1 + m1 * m2; i++)
          {
            int t_map2row = i / m1;
            int t_map2col = k1 + i % m1;
            int t_block_id = x * k1 * k2 + i;
            std::string t_block_key = "Stripe" + std::to_string(l_stripe_id) + "_R" + std::to_string(i);
            int t_map2node = r_node_ids[i];
            char t_block_type = 'R';
            if (i >= k2 * m1)
            {
              int j = i - k2 * m1;
              t_map2row = k2 + j / m1;
              t_map2col = x * k1 + j % m1;
              t_block_id = (x * k1 + m1) * k2 + x * k1 * m2 + j;
              t_block_key = "Stripe" + std::to_string(l_stripe_id) + "_G" + std::to_string(j);
              t_map2node = g_node_ids[j];
              t_block_type = 'G';
            }
            int t_map2rack = m_node_table[t_map2node].map2rack;
            Block *t_block = new Block(t_block_id, t_block_key, t_block_type, block_size, t_map2row, t_map2col,
                                       l_stripe_id, t_map2rack, t_map2node, "");
            if (IF_DEBUG)
            {
              std::cout << "\033[1;33m" << t_block->block_key << ": Rack" << t_block->map2rack << ", Node" << t_block->map2node << "\033[0m" << std::endl;
            }
            larger_stripe.blocks.push_back(t_block);
            update_stripe_info_in_node(true, t_map2node, l_stripe_id);
            m_rack_table[t_map2rack].stripes.insert(l_stripe_id);
            Rack &t_rack = m_rack_table[t_map2rack];
            t_rack.blocks.push_back(t_block);

            auto it = std::find(t_rack.nodes.begin(), t_rack.nodes.end(), t_map2node);
            if (it == t_rack.nodes.end())
            {
              std::cout << "[Generate new parity block] the selected node not in the selected rack!" << std::endl;
            }

            Node &t_node = m_node_table[t_map2node];
            proxy_proto::MergePlanHPC &t_plan = merge_plan[t_map2rack];
            t_plan.add_n_datanodeip(t_node.node_ip);
            t_plan.add_n_datanodeport(t_node.node_port);
            t_plan.add_n_blockkeys(t_block_key);
            t_plan.add_n_blockidxs(i);
          }
        }

        if (IF_DEBUG)
        {
          for (auto it = merge_plan.begin(); it != merge_plan.end(); it++)
          {
            std::cout << "Rack " << it->first << " : ";
            proxy_proto::MergePlanHPC &t_plan = merge_plan[it->first];
            for (int j = 0; j < int(t_plan.n_blockkeys_size()); j++)
            {
              std::cout << "[" << t_plan.n_blockkeys(j) << "]" << t_plan.n_blockidxs(j) << " ";
            }
            std::cout << std::endl;
          }
        }

        if (IF_DEBUG)
        {
          // print the result
          std::cout << std::endl;
          std::cout << "Data placement before data block relocation:" << std::endl;
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
        }

        // find out block to relocate
        // if there are more than one blocks from the same stripes placed in the same node
        int num_of_block_to_move = 0;
        std::vector<std::string> block_to_move_key;
        std::vector<int> block_src_node;
        std::vector<int> block_des_node;
        for (auto it = m_rack_table.begin(); it != m_rack_table.end(); it++)
        {
          Rack &t_rack = it->second;
          for (int i = 0; i < int(t_rack.nodes.size()); i++)
          {
            int t_node_id = t_rack.nodes[i];
            Node &t_node = m_node_table[t_node_id];
            auto it_n = t_node.stripes.find(l_stripe_id);
            if (it_n != t_node.stripes.end())
            {
              int block_num = it_n->second;
              // std::cout << it->first << "|" << t_node_id << "|" << l_stripe_id << "|" << block_num << " ";
              if (block_num > 1)
              {
                int num_to_move = block_num - 1;
                num_of_block_to_move += num_to_move;
                for (int j = 0; j < int(t_rack.blocks.size()) && num_to_move > 0; j++)
                {
                  Block *t_block = t_rack.blocks[j];
                  if (t_block->map2stripe == l_stripe_id && t_block->map2node == t_node_id)
                  {
                    int n_node_id = randomly_select_a_node_in_rack(t_rack.rack_id, l_stripe_id);
                    t_block->map2node = n_node_id;
                    block_to_move_key.push_back(t_block->block_key);
                    block_src_node.push_back(t_node_id);
                    block_des_node.push_back(n_node_id);
                    update_stripe_info_in_node(false, t_node_id, l_stripe_id);
                    update_stripe_info_in_node(true, n_node_id, l_stripe_id);
                    num_to_move--;
                  }
                }
              }
            }
          }
        }
        std::cout << std::endl;
        if (IF_DEBUG)
        {
          std::cout << "[MERGE] all blocks to relocate:";
          for (int ii = 0; ii < int(block_to_move_key.size()); ii++)
          {
            std::cout << block_to_move_key[ii] << "[" << block_src_node[ii] << "->" << block_des_node[ii] << "] ";
          }
          std::cout << std::endl;
        }
        gettimeofday(&end_time, NULL);
        m_meta_access_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

        // time
        double temp_time = 0.0;
        struct timeval r_start_time, r_end_time;
        struct timeval m_start_time, m_end_time;

        if (IF_DEBUG)
        {
          std::cout << "[MERGE] Start to recalculate parity blocks for Stripe" << l_stripe_id << std::endl;
        }
        temp_time = 0.0;
        gettimeofday(&r_start_time, NULL);

        // to recalculate row or col parities and global parities by simply xor
        auto send_merge_plan = [this, block_size, merge_plan, isvertical, x, l_stripe_id](int t_rack_id) mutable
        {
          proxy_proto::MergePlanHPC &t_plan = merge_plan[t_rack_id];
          t_plan.set_block_size(block_size);
          t_plan.set_isvertical(isvertical);
          t_plan.set_x(x);
          t_plan.set_stripe_id(l_stripe_id);
          grpc::ClientContext context_m;
          proxy_proto::RecalReply response_m;
          std::string chosen_proxy_m = m_rack_table[t_rack_id].proxy_ip + ":" + std::to_string(m_rack_table[t_rack_id].proxy_port);
          grpc::Status stat1 = m_proxy_ptrs[chosen_proxy_m]->handleMergeHPC(&context_m, t_plan, &response_m);
          if (IF_DEBUG)
          {
            std::cout << "Selected merge proxy " << chosen_proxy_m << std::endl;
          }
        };
        try
        {
          if (IF_DEBUG)
          {
            std::cout << "[Parities Recalculation] Send merge plans!" << std::endl;
          }
          std::vector<std::thread> senders;
          for (auto itm = merge_plan.begin(); itm != merge_plan.end(); itm++)
          {
            senders.push_back(std::thread(send_merge_plan, itm->first));
          }
          for (int j = 0; j < int(senders.size()); j++)
          {
            senders[j].join();
          }
          // check
          for (auto itm = merge_plan.begin(); itm != merge_plan.end(); itm++)
          {
            int t_rack_id = itm->first;
            proxy_proto::AskIfSuccess ask_c0;
            ask_c0.set_step(0);
            grpc::ClientContext context_c0;
            proxy_proto::RepIfSuccess response_c0;
            std::string chosen_proxy_c0 = m_rack_table[t_rack_id].proxy_ip + ":" + std::to_string(m_rack_table[t_rack_id].proxy_port);
            grpc::Status stat_c0 = m_proxy_ptrs[chosen_proxy_c0]->checkStep(&context_c0, ask_c0, &response_c0);
            if (stat_c0.ok() && response_c0.ifsuccess() && IF_DEBUG)
            {
              std::cout << "[MERGE] parity block recalculate success in Rack" << t_rack_id << std::endl;
            }
            m_cross_rack_time += response_c0.cross_rack_time();
            m_inner_rack_time += response_c0.inner_rack_time();
            m_encoding_time += response_c0.encoding_time();
          }
        }
        catch (const std::exception &e)
        {
          std::cerr << e.what() << '\n';
        }

        // to delete old parities
        if (IF_DEBUG)
        {
          std::cout << "[MERGE] Start to delete old parities for Stripe" << l_stripe_id << std::endl;
        }
        grpc::ClientContext del_context;
        proxy_proto::DelReply del_reply;
        old_parities.set_stripe_id(-1);
        old_parities.set_key("");
        // randomly select a proxy
        int idx = rand_num(int(old_parities_rack_set.size()));
        int del_rack_id = *(std::next(old_parities_rack_set.begin(), idx));
        std::string del_chosen_proxy = m_rack_table[del_rack_id].proxy_ip + ":" + std::to_string(m_rack_table[del_rack_id].proxy_port);
        grpc::Status del_status = m_proxy_ptrs[del_chosen_proxy]->deleteBlock(&del_context, old_parities, &del_reply);
        if (del_status.ok() && del_reply.ifcommit())
        {
          std::cout << "[MERGE] Delete old parity blocks success!" << std::endl;
        }
        gettimeofday(&r_end_time, NULL);
        temp_time = r_end_time.tv_sec - r_start_time.tv_sec + (r_end_time.tv_usec - r_start_time.tv_usec) * 1.0 / 1000000;
        t_rc += temp_time;

        // to relocate blocks
        if (int(block_to_move_key.size()) > 0)
        {
          if (IF_DEBUG)
          {
            std::cout << "[MERGE] Start to relocate data blocks for Stripe" << l_stripe_id << std::endl;
          }
          temp_time = 0.0;
          gettimeofday(&m_start_time, NULL);
          proxy_proto::blockRelocPlan b_reloc_plan;
          for (int i = 0; i < int(block_to_move_key.size()); i++)
          {
            int src_node_id = block_src_node[i];
            int des_node_id = block_des_node[i];
            b_reloc_plan.add_blocktomove(block_to_move_key[i]);
            b_reloc_plan.add_fromdatanodeip(m_node_table[src_node_id].node_ip);
            b_reloc_plan.add_fromdatanodeport(m_node_table[src_node_id].node_port);
            b_reloc_plan.add_todatanodeip(m_node_table[des_node_id].node_ip);
            b_reloc_plan.add_todatanodeport(m_node_table[des_node_id].node_port);
          }
          b_reloc_plan.set_block_size(block_size);
          int idx = rand_num(int(old_parities_rack_set.size()));
          int r_rack_id = *(std::next(old_parities_rack_set.begin(), idx));
          std::string chosen_proxy_b = m_rack_table[r_rack_id].proxy_ip + ":" + std::to_string(m_rack_table[r_rack_id].proxy_port);
          grpc::ClientContext context_b;
          proxy_proto::blockRelocReply response_b;
          grpc::Status stat_b = m_proxy_ptrs[chosen_proxy_b]->blockReloc(&context_b, b_reloc_plan, &response_b);
          // check
          proxy_proto::AskIfSuccess ask_c2;
          ask_c2.set_step(2);
          grpc::ClientContext context_c2;
          proxy_proto::RepIfSuccess response_c2;
          grpc::Status stat_c2 = m_proxy_ptrs[chosen_proxy_b]->checkStep(&context_c2, ask_c2, &response_c2);
          if (stat_c2.ok() && response_c2.ifsuccess() && IF_DEBUG)
          {
            std::cout << "[MERGE] block relocaltion success!" << std::endl;
          }
          gettimeofday(&m_end_time, NULL);
          temp_time = m_end_time.tv_sec - m_start_time.tv_sec + (m_end_time.tv_usec - m_start_time.tv_usec) * 1.0 / 1000000;
          t_mc += temp_time;
          m_cross_rack_time += response_c2.cross_rack_time();
          m_inner_rack_time += response_c2.inner_rack_time();
        }

        gettimeofday(&start_time, NULL);
        // update stripes meta information
        for (auto iter = old_stripe_id_set.begin(); iter != old_stripe_id_set.end(); iter++)
        {
          auto its = m_stripe_table.find(*iter);
          if (its != m_stripe_table.end())
          {
            m_stripe_table.erase(its);
          }
        }
        m_stripe_table[l_stripe_id] = larger_stripe;
        m_cur_stripe_id++;
        s_merge_group.push_back(l_stripe_id);
        stripe_cnt += x;
        gettimeofday(&end_time, NULL);
        m_meta_access_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

        std::cout << "[Merging Stage " << m_merge_degree + 1 << "] Process " << stripe_cnt << "/" << tot_stripe_num
                  << " rc:" << t_rc << " mc:" << t_mc << std::endl;
      }
      new_merge_groups.push_back(s_merge_group);
    }
    m_merge_groups.clear();
    m_merge_groups = new_merge_groups;
    m_merge_degree += 1;
    mergeReplyClient->set_ifmerged(true);
    mergeReplyClient->set_gc(t_rc);
    mergeReplyClient->set_dc(t_mc);
    mergeReplyClient->set_cross_rack_num(t_cross_rack);
    mergeReplyClient->set_cross_rack_time(m_cross_rack_time);
    mergeReplyClient->set_inner_rack_time(m_inner_rack_time);
    mergeReplyClient->set_encoding_time(m_encoding_time);
    mergeReplyClient->set_meta_time(m_meta_access_time);
    m_cross_rack_time = 0;
    m_inner_rack_time = 0;
    m_encoding_time = 0;
    m_meta_access_time = 0;

    if (IF_DEBUG)
    {
      // print the result
      std::cout << std::endl;
      std::cout << "After Merging:" << std::endl;
      for (int j = 0; j < m_num_of_Racks; j++)
      {
        Rack &t_rack = m_rack_table[j];
        if (int(t_rack.blocks.size()) > 0)
        {
          std::cout << "Rack " << j << ": ";
          for (auto it = t_rack.blocks.begin(); it != t_rack.blocks.end(); it++)
          {
            std::cout << "[" << (*it)->block_key << ":" << (*it)->block_id << "S" << (*it)->map2stripe << "R" << (*it)->map2row << "C" << (*it)->map2col << "N" << (*it)->map2node << "] ";
          }
          std::cout << std::endl;
        }
      }
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
      std::cout << std::endl;
    }
  }

  void CoordinatorImpl::simulation_recalculate(proxy_proto::mainRecalPlan &main_recal, int main_rack_id, int &cross_rack_num)
  {
    int parity_num = int(main_recal.p_blockkeys_size());
    for (int j = 0; j < int(main_recal.racks_size()); j++)
    {
      int t_rack_id = main_recal.racks(j).rack_id();
      if (main_rack_id != t_rack_id) // cross-rack
      {
        int help_block_num = int(main_recal.racks(j).blockkeys_size());
        if (help_block_num > parity_num && m_encode_parameters.partial_decoding)
        {
          cross_rack_num += parity_num;
        }
        else
        {
          cross_rack_num += help_block_num;
        }
      }
    }
  }
}