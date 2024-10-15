#include "coordinator.h"
#include "tinyxml2.h"
#include <random>
#include <algorithm>
#include <unistd.h>
#include <sys/time.h>

namespace ECProject
{
  void CoordinatorImpl::request_repair(bool isblock, int stripe_id, std::shared_ptr<std::vector<int>> blocks_or_nodes, coordinator_proto::RepIfRepaired *repairReplyClient)
  {
    ECProject::EncodeType encodetype = m_encode_parameters.encodetype;
    auto failure_map = std::make_shared<std::map<int, std::vector<Block *>>>();
    auto failures_type = std::make_shared<std::map<int, ECProject::FailureType>>();

    check_out_failures(isblock, stripe_id, blocks_or_nodes, failure_map, failures_type);

    double t_rc = 0.0;
    double temp_time = 0.0;
    struct timeval r_start_time, r_end_time;
    int tot_stripe_num = int(failure_map->size());
    int stripe_cnt = 0;
    int failed_cnt = 0;
    // for simulation
    int t_cross_rack = 0;

    for (auto it1 = failure_map->begin(); it1 != failure_map->end(); it1++)
    {
      gettimeofday(&start_time, NULL);
      int t_stripe_id = it1->first;
      failed_cnt += int((it1->second).size());
      auto it2 = failures_type->find(t_stripe_id);
      ECProject::FailureType ft = it2->second;
      if (IF_DEBUG)
      {
        std::cout << std::endl;
        std::cout << "[Type " << ft << "] Failed Stripe " << t_stripe_id << " (f" << (it1->second).size() << ") : ";
        for (auto t_it = (it1->second).begin(); t_it != (it1->second).end(); t_it++)
        {
          std::cout << (*t_it)->block_key << " ";
        }
        std::cout << std::endl;
      }
      std::vector<proxy_proto::mainRepairPlan> main_repair;
      std::vector<std::vector<proxy_proto::helpRepairPlan>> help_repair;
      bool flag = true;
      if (encodetype == HPC)
      {
        if (ft == Single_Block)
        {
          flag = generate_repair_plan_for_single_block_hpc(main_repair, help_repair, t_stripe_id, it1->second);
        }
        else if (ft == Rand_Multi_Blocks)
        {
          flag = generate_repair_plan_for_rand_multi_blocks_hpc(main_repair, help_repair, t_stripe_id, it1->second);
        }
        else
        {
          flag = generate_repair_plan_for_single_rack_hpc(main_repair, help_repair, t_stripe_id, it1->second);
        }
      }
      else if (encodetype == MLEC)
      {
        if (ft == Single_Block)
        {
          flag = generate_repair_plan_for_single_block_mlec(main_repair, help_repair, t_stripe_id, it1->second);
        }
        else
        {
          flag = generate_repair_plan_for_rand_multi_blocks_mlec(main_repair, help_repair, t_stripe_id, it1->second);
        }
      }
      else if (encodetype == Azure_LRC)
      {
        if (ft == Single_Block)
        {
          flag = generate_repair_plan_for_single_block_lrc(main_repair, help_repair, t_stripe_id, it1->second);
        }
        else
        {
          flag = generate_repair_plan_for_multi_blocks_lrc(main_repair, help_repair, t_stripe_id, it1->second);
        }
      }
      else
      {
        flag = generate_repair_plan_for_rs(main_repair, help_repair, t_stripe_id, it1->second);
      }

      auto main_decoding_time = std::make_shared<std::vector<double>>();
      auto help_decoding_time = std::make_shared<std::vector<double>>();

      auto send_main_repair_plan = [this, main_repair, main_decoding_time](int i, int m_rack_id) mutable
      {
        grpc::ClientContext context_m;
        proxy_proto::RepairReply response_m;
        std::string chosen_proxy_m = m_rack_table[m_rack_id].proxy_ip + ":" + std::to_string(m_rack_table[m_rack_id].proxy_port);
        grpc::Status stat1 = m_proxy_ptrs[chosen_proxy_m]->mainRepair(&context_m, main_repair[i], &response_m);
        main_decoding_time->push_back(response_m.time());
        if (IF_DEBUG)
        {
          std::cout << "Selected main proxy " << chosen_proxy_m << " of Rack" << m_rack_id << std::endl;
        }
      };

      auto send_help_repair_plan = [this, main_repair, help_repair, help_decoding_time](int i, int j, std::string proxy_ip, int proxy_port) mutable
      {
        grpc::ClientContext context_h;
        proxy_proto::RepairReply response_h;
        std::string chosen_proxy_h = proxy_ip + ":" + std::to_string(proxy_port);
        grpc::Status stat1 = m_proxy_ptrs[chosen_proxy_h]->helpRepair(&context_h, help_repair[i][j], &response_h);
        help_decoding_time->push_back(response_h.time());
        if (IF_DEBUG)
        {
          std::cout << "Selected help proxy " << chosen_proxy_h << std::endl;
        }
      };
      gettimeofday(&end_time, NULL);
      m_meta_access_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

      // simulation
      if (flag)
      {
        simulation_repair(main_repair, t_cross_rack);
      }

      temp_time = 0.0;
      gettimeofday(&r_start_time, NULL);
      if (!flag)
      {
        std::cout << "Undecodable!" << std::endl;
        // failed_cnt++;
      }
      else
      {
        for (int i = 0; i < int(main_repair.size()); i++)
        {
          if (IF_DEBUG)
          {
            std::cout << "> Failed Blocks: ";
            for (int j = 0; j < int(main_repair[i].failed_blockkeys_size()); j++)
            {
              std::cout << main_repair[i].failed_blockkeys(j) << " ";
            }
            std::cout << std::endl;
            std::cout << "> Repair by Blocks: ";
            for (int j = 0; j < int(main_repair[i].racks_size()); j++)
            {
              std::cout << "[Rack" << main_repair[i].racks(j).rack_id() << "][" << int(main_repair[i].racks(j).blockkeys_size()) << "]";
              for (int jj = 0; jj < int(main_repair[i].racks(j).blockkeys_size()); jj++)
              {
                std::cout << main_repair[i].racks(j).blockkeys(jj) << " ";
              }
            }
            std::cout << std::endl;
            std::cout << "> Parity Blocks: ";
            for (int j = 0; j < int(main_repair[i].parity_blockids_size()); j++)
            {
              std::cout << main_repair[i].parity_blockids(j) << " ";
            }
            std::cout << std::endl;
          }

          try
          {
            int m_rack_id = main_repair[i].m_rack_id();
            std::thread my_main_thread(send_main_repair_plan, i, m_rack_id);
            std::vector<std::thread> senders;
            int index = 0;
            for (int j = 0; j < int(main_repair[i].racks_size()); j++)
            {
              int t_rack_id = main_repair[i].racks(j).rack_id();
              if (IF_DEBUG)
              {
                std::cout << int(help_repair[i].size()) << "|" << m_rack_id << "|" << t_rack_id << " ";
              }
              if (t_rack_id != m_rack_id)
              {
                std::string proxy_ip = main_repair[i].racks(j).proxy_ip();
                int proxy_port = main_repair[i].racks(j).proxy_port();
                if (IF_DEBUG)
                {
                  std::cout << proxy_ip << ":" << proxy_port << " ";
                }
                senders.push_back(std::thread(send_help_repair_plan, i, index, proxy_ip, proxy_port));
                index++;
              }
            }
            if (IF_DEBUG)
            {
              std::cout << std::endl;
            }
            for (int j = 0; j < int(senders.size()); j++)
            {
              senders[j].join();
            }
            my_main_thread.join();

            // check
            proxy_proto::AskIfSuccess ask_c0;
            ask_c0.set_step(0);
            grpc::ClientContext context_c0;
            proxy_proto::RepIfSuccess response_c0;
            std::string chosen_proxy_c0 = m_rack_table[m_rack_id].proxy_ip + ":" + std::to_string(m_rack_table[m_rack_id].proxy_port);
            grpc::Status stat_c0 = m_proxy_ptrs[chosen_proxy_c0]->checkStep(&context_c0, ask_c0, &response_c0);
            if (stat_c0.ok() && response_c0.ifsuccess() && IF_DEBUG)
            {
              std::cout << "[Repair] block repair success in Rack" << m_rack_id << std::endl;
            }
            else if (IF_DEBUG)
            {
              std::cout << "\033[1;37m[Repair] Failed here!!! In Rack" << m_rack_id << "\033[0m" << std::endl;
              exit(0);
            }
            m_cross_rack_time += response_c0.cross_rack_time();
            m_inner_rack_time += response_c0.inner_rack_time();

            m_decoding_time += (*main_decoding_time)[0];
            main_decoding_time->clear();
            if ((int)help_decoding_time->size() > 0)
            {
              double temp_time = 0.0;
              for (auto it = help_decoding_time->begin(); it != help_decoding_time->end(); it++)
              {
                temp_time += *it;
              }
              m_decoding_time += temp_time / (double)help_decoding_time->size();
              help_decoding_time->clear();
            }
          }
          catch (const std::exception &e)
          {
            std::cerr << e.what() << '\n';
          }
        }
      }
      gettimeofday(&r_end_time, NULL);
      temp_time = r_end_time.tv_sec - r_start_time.tv_sec + (r_end_time.tv_usec - r_start_time.tv_usec) * 1.0 / 1000000;
      t_rc += temp_time;

      stripe_cnt++;
      // if(IF_DEBUG)
      // {
      std::cout << "[Repair] Process " << stripe_cnt << "/" << tot_stripe_num << "  rc:" << t_rc << " dc:" << m_decoding_time << std::endl;
      // }
    }
    std::cout << "[Failures] " << failed_cnt << std::endl
              << std::endl;
    repairReplyClient->set_ifrepaired(true);
    repairReplyClient->set_rc(t_rc);
    repairReplyClient->set_failed_stripe_num(failed_cnt);
    repairReplyClient->set_cross_rack_num(t_cross_rack);
    repairReplyClient->set_decoding_time(m_decoding_time);
    repairReplyClient->set_cross_rack_time(m_cross_rack_time);
    repairReplyClient->set_inner_rack_time(m_inner_rack_time);
    repairReplyClient->set_meta_time(m_meta_access_time);
    m_cross_rack_time = 0;
    m_inner_rack_time = 0;
    m_decoding_time = 0;
    m_meta_access_time = 0;
  }

  void CoordinatorImpl::check_out_failures(
      bool isblock, int stripe_id, std::shared_ptr<std::vector<int>> failureinfo,
      std::shared_ptr<std::map<int, std::vector<Block *>>> failure_map,
      std::shared_ptr<std::map<int, ECProject::FailureType>> failures_type)
  {
    if (isblock)
    {
      Stripe &t_stripe = m_stripe_table[stripe_id];
      std::vector<Block *> t_blocks_list;
      for (int i = 0; i < int(failureinfo->size()); i++)
      {
        int t_block_id = (*failureinfo)[i];
        for (int j = 0; j < int(t_stripe.blocks.size()); j++)
        {
          if (t_stripe.blocks[j]->block_id == t_block_id)
          {
            t_blocks_list.push_back(t_stripe.blocks[j]);
            break;
          }
        }
      }
      (*failure_map)[stripe_id] = t_blocks_list;
      if (int(failureinfo->size()) == 1)
      {
        (*failures_type)[stripe_id] = ECProject::Single_Block;
      }
      else
      {
        (*failures_type)[stripe_id] = check_out_failure_type(t_blocks_list, stripe_id);
      }
    }
    else
    {
      for (int i = 0; i < int(failureinfo->size()); i++)
      {
        int t_node_id = (*failureinfo)[i];
        int t_rack_id = m_node_table[t_node_id].map2rack;
        Rack &t_rack = m_rack_table[t_rack_id];
        for (int j = 0; j < int(t_rack.blocks.size()); j++)
        {
          if (t_rack.blocks[j]->map2node == t_node_id)
          {
            int t_stripe_id = t_rack.blocks[j]->map2stripe;
            auto it = failure_map->find(t_stripe_id);
            if (it == failure_map->end())
            {
              std::vector<Block *> tmp;
              tmp.push_back(t_rack.blocks[j]);
              failure_map->insert(std::make_pair(t_stripe_id, tmp));
            }
            else
            {
              (it->second).push_back(t_rack.blocks[j]);
            }
          }
        }
      }
      // if(IF_DEBUG)
      // {
      //   std::cout << std::endl << "Failure Map:" << std::endl;
      //   for(auto it = failure_map->begin(); it != failure_map->end(); it++)
      //   {
      //     int t_stripe_id = it->first;
      //     std::cout << "Stripe " << t_stripe_id << ": ";
      //     std::vector<Block *> &failed_list = it->second;
      //     for(int j = 0; j < int(failed_list.size()); j++)
      //     {
      //       std::cout << failed_list[j]->block_key << " ";
      //     }
      //     std::cout << std::endl;
      //   }
      // }
      for (auto it = failure_map->begin(); it != failure_map->end(); it++)
      {
        int t_stripe_id = it->first;
        if (int((it->second).size()) == 1)
        {
          (*failures_type)[t_stripe_id] = ECProject::Single_Block;
        }
        else
        {
          (*failures_type)[t_stripe_id] = check_out_failure_type(it->second, t_stripe_id);
        }
      }
    }
  }

  ECProject::FailureType CoordinatorImpl::check_out_failure_type(std::vector<Block *> &failed_block_list, int stripe_id)
  {
    std::map<int, int> rack_blocks;
    std::unordered_set<int> t_rack_set;
    block_num_in_rack(rack_blocks, stripe_id);
    for (int i = 0; i < int(failed_block_list.size()); i++)
    {
      int t_rack_id = failed_block_list[i]->map2rack;
      rack_blocks[t_rack_id] -= 1;
      t_rack_set.insert(t_rack_id);
    }
    if (int(t_rack_set.size()) > 1)
    {
      return ECProject::Rand_Multi_Blocks;
    }
    else
    {
      int failed_rack_num = int(t_rack_set.size());
      if (failed_rack_num > 1)
      {
        return ECProject::Rand_Multi_Blocks;
      }
      else
      {
        if (rack_blocks[failed_block_list[0]->map2rack] == 0)
        {
          return ECProject::Single_Rack;
        }
        else
        {
          return ECProject::Rand_Multi_Blocks;
        }
      }
    }
  }

  // to record main proxy address for coordinator

  bool CoordinatorImpl::generate_repair_plan_for_single_block_hpc(
      std::vector<proxy_proto::mainRepairPlan> &main_repair,
      std::vector<std::vector<proxy_proto::helpRepairPlan>> &help_repair,
      int stripe_id, std::vector<Block *> &failed_blocks)
  {
    proxy_proto::mainRepairPlan t_main_plan;
    int x = m_encode_parameters.x_stripepermergegroup;
    Block *t_block_ptr = failed_blocks[0];
    int block_size = t_block_ptr->block_size;
    int t_map2rack = t_block_ptr->map2rack;
    int t_map2col = t_block_ptr->map2col;
    Stripe &t_stripe = m_stripe_table[stripe_id];
    int k2 = t_stripe.k2;
    int m2 = t_stripe.m2;
    auto blocks_location = t_main_plan.add_racks();
    blocks_location->set_rack_id(t_map2rack);
    blocks_location->set_proxy_ip(m_rack_table[t_map2rack].proxy_ip);
    blocks_location->set_proxy_port(m_rack_table[t_map2rack].proxy_port);
    int cnt = 0;
    for (int i = 0; i < int(t_stripe.blocks.size()); i++)
    {
      Block *block_ptr = t_stripe.blocks[i];
      if (block_ptr->map2col == t_map2col && block_ptr->block_id != t_block_ptr->block_id)
      {
        blocks_location->add_blockkeys(block_ptr->block_key);
        blocks_location->add_blockids(block_ptr->map2row);
        int node_id = block_ptr->map2node;
        Node &t_node = m_node_table[node_id];
        blocks_location->add_datanodeip(t_node.node_ip);
        blocks_location->add_datanodeport(t_node.node_port);
        cnt++;
      }
      if (cnt == k2)
        break;
    }
    t_main_plan.add_failed_blockkeys(t_block_ptr->block_key);
    t_main_plan.add_failed_blockids(t_block_ptr->map2row);
    t_main_plan.add_failed_datanodeip(m_node_table[t_block_ptr->map2node].node_ip);
    t_main_plan.add_failed_datanodeport(m_node_table[t_block_ptr->map2node].node_port);
    t_main_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
    t_main_plan.set_approach(m_encode_parameters.approach);
    t_main_plan.set_k(k2);
    t_main_plan.set_m_g(m2);
    t_main_plan.set_x_l(x);
    t_main_plan.set_seri_num(stripe_id % x);
    t_main_plan.set_block_size(block_size);
    t_main_plan.set_encodetype(m_encode_parameters.encodetype);
    if (m_encode_parameters.m_stripe_placementtype == ECProject::Horizontal)
      t_main_plan.set_rv_or_ch__isglobal(true);
    else
      t_main_plan.set_rv_or_ch__isglobal(false);
    t_main_plan.set_m_rack_id(t_map2rack);
    main_repair.push_back(t_main_plan);
    std::vector<proxy_proto::helpRepairPlan> t_help_plans;
    help_repair.push_back(t_help_plans);
    return true;
  }

  bool CoordinatorImpl::generate_repair_plan_for_rand_multi_blocks_hpc(
      std::vector<proxy_proto::mainRepairPlan> &main_repair,
      std::vector<std::vector<proxy_proto::helpRepairPlan>> &help_repair,
      int stripe_id, std::vector<Block *> &failed_blocks)
  {
    Stripe &t_stripe = m_stripe_table[stripe_id];
    int k1 = t_stripe.k1;
    int m1 = t_stripe.m1;
    int k2 = t_stripe.k2;
    int m2 = t_stripe.m2;
    int x = m_encode_parameters.x_stripepermergegroup;

    std::vector<std::vector<int>> failed_map(k2 + m2, std::vector<int>(k1 + m1, 0));
    std::vector<std::vector<Block *>> blocks_map(k2 + m2, std::vector<Block *>(k1 + m1, nullptr));
    std::vector<int> fb_row_cnt(k2 + m2, 0);
    std::vector<int> fb_col_cnt(k1 + m1, 0);

    int failed_blocks_num = int(failed_blocks.size());
    for (int i = 0; i < failed_blocks_num; i++)
    {
      int map2row = failed_blocks[i]->map2row;
      int map2col = failed_blocks[i]->map2col;
      failed_map[map2row][map2col] = 1;
      fb_row_cnt[map2row] += 1;
      fb_col_cnt[map2col] += 1;
    }
    for (int i = 0; i < int(t_stripe.blocks.size()); i++)
    {
      int map2row = t_stripe.blocks[i]->map2row;
      int map2col = t_stripe.blocks[i]->map2col;
      blocks_map[map2row][map2col] = t_stripe.blocks[i];
    }
    while (failed_blocks_num > 0)
    {
      // part one
      for (int i = 0; i < k1 + m1; i++)
      {
        if (fb_col_cnt[i] <= m2 && fb_col_cnt[i] > 0) // inner-rack
        {
          int cnt = 0;
          int t_map2rack = blocks_map[0][i]->map2rack;
          int block_size = blocks_map[0][i]->block_size;
          proxy_proto::mainRepairPlan t_main_plan;
          auto blocks_location = t_main_plan.add_racks();
          blocks_location->set_rack_id(t_map2rack);
          blocks_location->set_proxy_ip(m_rack_table[t_map2rack].proxy_ip);
          blocks_location->set_proxy_port(m_rack_table[t_map2rack].proxy_port);
          for (int j = 0; j < k2 + m2; j++)
          {
            if (failed_map[j][i])
            {
              t_main_plan.add_failed_blockkeys(blocks_map[j][i]->block_key);
              t_main_plan.add_failed_blockids(blocks_map[j][i]->map2row);
              int t_node_id = blocks_map[j][i]->map2node;
              t_main_plan.add_failed_datanodeip(m_node_table[t_node_id].node_ip);
              t_main_plan.add_failed_datanodeport(m_node_table[t_node_id].node_port);
            }
            else if (cnt < k2)
            {
              blocks_location->add_blockkeys(blocks_map[j][i]->block_key);
              blocks_location->add_blockids(blocks_map[j][i]->map2row);
              int t_node_id = blocks_map[j][i]->map2node;
              blocks_location->add_datanodeip(m_node_table[t_node_id].node_ip);
              blocks_location->add_datanodeport(m_node_table[t_node_id].node_port);
              cnt++;
            }
          }
          t_main_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
          t_main_plan.set_approach(m_encode_parameters.approach);
          t_main_plan.set_k(k2);
          t_main_plan.set_m_g(m2);
          t_main_plan.set_x_l(x);
          t_main_plan.set_seri_num(stripe_id % x);
          t_main_plan.set_block_size(block_size);
          t_main_plan.set_encodetype(m_encode_parameters.encodetype);
          if (m_encode_parameters.m_stripe_placementtype == ECProject::Horizontal)
            t_main_plan.set_rv_or_ch__isglobal(true);
          else
            t_main_plan.set_rv_or_ch__isglobal(false);
          t_main_plan.set_m_rack_id(t_map2rack);
          main_repair.push_back(t_main_plan);
          std::vector<proxy_proto::helpRepairPlan> t_help_plans;
          help_repair.push_back(t_help_plans);
          // update failed_map
          for (int jj = 0; jj < k2 + m2; jj++)
          {
            if (failed_map[jj][i])
            {
              failed_map[jj][i] = 0;
              failed_blocks_num -= 1;
              fb_row_cnt[jj] -= 1;
              fb_col_cnt[i] -= 1;
            }
          }
        }
      }
      if (failed_blocks_num == 0)
      {
        break;
      }
      // part two
      int max_row = -1;
      int max_block_num = -1;
      for (int i = 0; i < k2 + m2; i++)
      {
        if (fb_row_cnt[i] > 0 && fb_row_cnt[i] <= m1 && max_block_num < fb_row_cnt[i])
        {
          max_block_num = fb_row_cnt[i];
          max_row = i;
        }
      }
      if (max_row == -1)
      {
        std::cout << "Undecodable!!" << std::endl;
        return false;
      }
      // optimize the selection of main proxy?
      int t_main_rack_id = -1;
      std::unordered_set<int> t_rack_set;
      proxy_proto::mainRepairPlan t_main_plan;
      int cnt = 0;
      int start_idx = 0;
      int stop_idx = 0;
      int block_size = 0;

      // for single-block
      for (int j = 0; j < k1 + m1; j++)
      {
        if (failed_map[max_row][j])
        {
          if (fb_row_cnt[max_row] == 1 && j >= k1 && !m_encode_parameters.partial_decoding)
          {
            start_idx = m1 - 1;
          }
          break;
        }
      }

      for (int i = start_idx; i < k1 + m1; i++)
      {
        if (failed_map[max_row][i])
        {
          t_main_plan.add_failed_blockkeys(blocks_map[max_row][i]->block_key);
          t_main_plan.add_failed_blockids(blocks_map[max_row][i]->map2col);
          int t_node_id = blocks_map[max_row][i]->map2node;
          t_main_plan.add_failed_datanodeip(m_node_table[t_node_id].node_ip);
          t_main_plan.add_failed_datanodeport(m_node_table[t_node_id].node_port);
          t_main_rack_id = m_node_table[t_node_id].map2rack;
          block_size = blocks_map[max_row][i]->block_size;
          if (i >= k1)
          {
            t_main_plan.add_parity_blockids(blocks_map[max_row][i]->map2col);
          }
        }
        else if (cnt < k1)
        {
          t_rack_set.insert(blocks_map[max_row][i]->map2rack);
          cnt++;
          if (i >= k1)
          {
            t_main_plan.add_parity_blockids(blocks_map[max_row][i]->map2col);
          }
          if (cnt == k1)
          {
            stop_idx = i;
          }
        }
      }
      std::vector<proxy_proto::helpRepairPlan> t_help_plans;
      for (auto it = t_rack_set.begin(); it != t_rack_set.end(); it++)
      {
        int t_rack_id = *it;
        proxy_proto::helpRepairPlan t_help_plan;
        auto blocks_location = t_main_plan.add_racks();
        blocks_location->set_rack_id(t_rack_id);
        t_help_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
        t_help_plan.set_mainproxyip(m_rack_table[t_main_rack_id].proxy_ip);
        t_help_plan.set_mainproxyport(m_rack_table[t_main_rack_id].proxy_port + 1);
        t_help_plan.set_k(k1);
        t_help_plan.set_m_g(m1);
        t_help_plan.set_x_l(x);
        t_help_plan.set_seri_num(stripe_id % x);
        t_help_plan.set_block_size(block_size);
        t_help_plan.set_encodetype(m_encode_parameters.encodetype);
        if (m_encode_parameters.m_stripe_placementtype == ECProject::Horizontal)
          t_help_plan.set_rv_or_ch__isglobal(false);
        else
          t_help_plan.set_rv_or_ch__isglobal(true);
        t_help_plan.set_failed_num(max_block_num);
        for (int i = start_idx; i <= stop_idx; i++)
        {
          if (t_rack_id == blocks_map[max_row][i]->map2rack && !failed_map[max_row][i])
          {
            int t_node_id = blocks_map[max_row][i]->map2node;
            t_help_plan.add_blockkeys(blocks_map[max_row][i]->block_key);
            t_help_plan.add_blockids(blocks_map[max_row][i]->map2col);
            t_help_plan.add_datanodeip(m_node_table[t_node_id].node_ip);
            t_help_plan.add_datanodeport(m_node_table[t_node_id].node_port);

            blocks_location->add_blockkeys(blocks_map[max_row][i]->block_key);
            blocks_location->add_blockids(blocks_map[max_row][i]->map2col);
            blocks_location->add_datanodeip(m_node_table[t_node_id].node_ip);
            blocks_location->add_datanodeport(m_node_table[t_node_id].node_port);
          }
        }
        for (int i = 0; i < int(t_main_plan.parity_blockids_size()); i++)
        {
          t_help_plan.add_parity_blockids(t_main_plan.parity_blockids(i));
        }
        if (t_rack_id != t_main_rack_id)
          t_help_plans.push_back(t_help_plan);

        blocks_location->set_proxy_ip(m_rack_table[t_rack_id].proxy_ip);
        blocks_location->set_proxy_port(m_rack_table[t_rack_id].proxy_port);
      }
      help_repair.push_back(t_help_plans);

      t_main_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
      t_main_plan.set_approach(m_encode_parameters.approach);
      t_main_plan.set_k(k1);
      t_main_plan.set_m_g(m1);
      t_main_plan.set_x_l(x);
      t_main_plan.set_seri_num(stripe_id % x);
      t_main_plan.set_block_size(block_size);
      t_main_plan.set_encodetype(m_encode_parameters.encodetype);
      if (m_encode_parameters.m_stripe_placementtype == ECProject::Horizontal)
        t_main_plan.set_rv_or_ch__isglobal(false);
      else
        t_main_plan.set_rv_or_ch__isglobal(true);
      t_main_plan.set_m_rack_id(t_main_rack_id);

      main_repair.push_back(t_main_plan);

      // update failed_map
      for (int i = 0; i < k1 + m1; i++)
      {
        if (failed_map[max_row][i])
        {
          failed_map[max_row][i] = 0;
          failed_blocks_num -= 1;
          fb_row_cnt[max_row] -= 1;
          fb_col_cnt[i] -= 1;
        }
      }
    }
    return true;
  }

  bool CoordinatorImpl::generate_repair_plan_for_single_rack_hpc(
      std::vector<proxy_proto::mainRepairPlan> &main_repair,
      std::vector<std::vector<proxy_proto::helpRepairPlan>> &help_repair,
      int stripe_id, std::vector<Block *> &failed_blocks)
  {
    Stripe &t_stripe = m_stripe_table[stripe_id];
    int k1 = t_stripe.k1;
    int m1 = t_stripe.m1;
    int k2 = t_stripe.k2;
    int m2 = t_stripe.m2;
    int x = m_encode_parameters.x_stripepermergegroup;
    bool approach = m_encode_parameters.approach;

    std::vector<std::vector<int>> failed_map(k2 + m2, std::vector<int>(k1 + m1, 0));
    std::vector<std::vector<Block *>> blocks_map(k2 + m2, std::vector<Block *>(k1 + m1, nullptr));
    std::vector<int> fb_row_cnt(k2 + m2, 0);
    std::vector<int> fb_col_cnt(k1 + m1, 0);

    int failed_blocks_num = int(failed_blocks.size());
    for (int i = 0; i < failed_blocks_num; i++)
    {
      int map2row = failed_blocks[i]->map2row;
      int map2col = failed_blocks[i]->map2col;
      failed_map[map2row][map2col] = 1;
      fb_row_cnt[map2row] += 1;
      fb_col_cnt[map2col] += 1;
    }
    for (int i = 0; i < int(t_stripe.blocks.size()); i++)
    {
      int map2row = t_stripe.blocks[i]->map2row;
      int map2col = t_stripe.blocks[i]->map2col;
      blocks_map[map2row][map2col] = t_stripe.blocks[i];
    }

    int repair_lines = k2 + m2;
    if (approach)
    {
      repair_lines = k2;
    }

    for (int i = 0; i < repair_lines; i++)
    {
      int t_main_rack_id = -1;
      std::unordered_set<int> t_rack_set;
      proxy_proto::mainRepairPlan t_main_plan;
      int cnt = 0;
      int start_idx = 0;
      int stop_idx = 0;
      int block_size = 0;

      // for single-rack, assume that k1 % m1 == 0 and each rack places m1 columns
      for (int j = 0; j < k1 + m1; j++)
      {
        if (failed_map[i][j])
        {
          if (fb_row_cnt[i] == 1 && j >= k1 && !m_encode_parameters.partial_decoding)
          {
            start_idx = m1 - 1;
          }
          break;
        }
      }

      for (int j = start_idx; j < k1 + m1; j++)
      {
        if (failed_map[i][j])
        {
          t_main_plan.add_failed_blockkeys(blocks_map[i][j]->block_key);
          t_main_plan.add_failed_blockids(blocks_map[i][j]->map2col);
          int t_node_id = blocks_map[i][j]->map2node;
          t_main_plan.add_failed_datanodeip(m_node_table[t_node_id].node_ip);
          t_main_plan.add_failed_datanodeport(m_node_table[t_node_id].node_port);
          t_main_rack_id = m_node_table[t_node_id].map2rack;
          block_size = blocks_map[i][j]->block_size;
          if (j >= k1)
          {
            t_main_plan.add_parity_blockids(blocks_map[i][j]->map2col);
          }
        }
        else if (cnt < k1)
        {
          t_rack_set.insert(blocks_map[i][j]->map2rack);
          cnt++;
          if (j >= k1)
          {
            t_main_plan.add_parity_blockids(blocks_map[i][j]->map2col);
          }
          if (cnt == k1)
          {
            stop_idx = j;
          }
        }
      }
      std::vector<proxy_proto::helpRepairPlan> t_help_plans;
      for (auto it = t_rack_set.begin(); it != t_rack_set.end(); it++)
      {
        int t_rack_id = *it;
        proxy_proto::helpRepairPlan t_help_plan;
        auto blocks_location = t_main_plan.add_racks();
        blocks_location->set_rack_id(t_rack_id);
        t_help_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
        t_help_plan.set_mainproxyip(m_rack_table[t_main_rack_id].proxy_ip);
        t_help_plan.set_mainproxyport(m_rack_table[t_main_rack_id].proxy_port + 1);
        t_help_plan.set_k(k1);
        t_help_plan.set_m_g(m1);
        t_help_plan.set_x_l(x);
        t_help_plan.set_seri_num(stripe_id % x);
        t_help_plan.set_block_size(block_size);
        t_help_plan.set_encodetype(m_encode_parameters.encodetype);
        if (m_encode_parameters.m_stripe_placementtype == ECProject::Horizontal)
          t_help_plan.set_rv_or_ch__isglobal(false);
        else
          t_help_plan.set_rv_or_ch__isglobal(true);
        t_help_plan.set_failed_num(fb_row_cnt[i]);
        for (int j = start_idx; j <= stop_idx; j++)
        {
          if (t_rack_id == blocks_map[i][j]->map2rack && !failed_map[i][j])
          {
            int t_node_id = blocks_map[i][j]->map2node;
            t_help_plan.add_blockkeys(blocks_map[i][j]->block_key);
            t_help_plan.add_blockids(blocks_map[i][j]->map2col);
            t_help_plan.add_datanodeip(m_node_table[t_node_id].node_ip);
            t_help_plan.add_datanodeport(m_node_table[t_node_id].node_port);

            blocks_location->add_blockkeys(blocks_map[i][j]->block_key);
            blocks_location->add_blockids(blocks_map[i][j]->map2col);
            blocks_location->add_datanodeip(m_node_table[t_node_id].node_ip);
            blocks_location->add_datanodeport(m_node_table[t_node_id].node_port);
          }
        }
        for (int j = 0; j < int(t_main_plan.parity_blockids_size()); j++)
        {
          t_help_plan.add_parity_blockids(t_main_plan.parity_blockids(j));
        }
        if (t_rack_id != t_main_rack_id)
          t_help_plans.push_back(t_help_plan);

        blocks_location->set_proxy_ip(m_rack_table[t_rack_id].proxy_ip);
        blocks_location->set_proxy_port(m_rack_table[t_rack_id].proxy_port);
      }
      help_repair.push_back(t_help_plans);

      t_main_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
      t_main_plan.set_approach(m_encode_parameters.approach);
      t_main_plan.set_k(k1);
      t_main_plan.set_m_g(m1);
      t_main_plan.set_x_l(x);
      t_main_plan.set_seri_num(stripe_id % x);
      t_main_plan.set_block_size(block_size);
      t_main_plan.set_encodetype(m_encode_parameters.encodetype);
      if (m_encode_parameters.m_stripe_placementtype == ECProject::Horizontal)
        t_main_plan.set_rv_or_ch__isglobal(false);
      else
        t_main_plan.set_rv_or_ch__isglobal(true);
      t_main_plan.set_m_rack_id(t_main_rack_id);

      main_repair.push_back(t_main_plan);

      // update failed_map
      for (int j = 0; j < k1 + m1; j++)
      {
        if (failed_map[i][j])
        {
          failed_map[i][j] = 0;
          failed_blocks_num -= 1;
          fb_row_cnt[i] -= 1;
          fb_col_cnt[j] -= 1;
        }
      }
    }

    if (approach) // repair column parity with the repaired data blocks inside rack
    {
      for (int i = 0; i < k1 + m1; i++)
      {
        if (fb_col_cnt[i] <= m2 && fb_col_cnt[i] > 0) // inner-rack
        {
          int cnt = 0;
          int t_map2rack = blocks_map[0][i]->map2rack;
          int block_size = blocks_map[0][i]->block_size;
          proxy_proto::mainRepairPlan t_main_plan;
          auto blocks_location = t_main_plan.add_racks();
          blocks_location->set_rack_id(t_map2rack);
          blocks_location->set_proxy_ip(m_rack_table[t_map2rack].proxy_ip);
          blocks_location->set_proxy_port(m_rack_table[t_map2rack].proxy_port);
          for (int j = 0; j < k2 + m2; j++)
          {
            if (failed_map[j][i])
            {
              t_main_plan.add_failed_blockkeys(blocks_map[j][i]->block_key);
              t_main_plan.add_failed_blockids(blocks_map[j][i]->map2row);
              int t_node_id = blocks_map[j][i]->map2node;
              t_main_plan.add_failed_datanodeip(m_node_table[t_node_id].node_ip);
              t_main_plan.add_failed_datanodeport(m_node_table[t_node_id].node_port);
            }
            else if (cnt < k2)
            {
              blocks_location->add_blockkeys(blocks_map[j][i]->block_key);
              blocks_location->add_blockids(blocks_map[j][i]->map2row);
              int t_node_id = blocks_map[j][i]->map2node;
              blocks_location->add_datanodeip(m_node_table[t_node_id].node_ip);
              blocks_location->add_datanodeport(m_node_table[t_node_id].node_port);
              cnt++;
            }
          }
          t_main_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
          t_main_plan.set_approach(m_encode_parameters.approach);
          t_main_plan.set_k(k2);
          t_main_plan.set_m_g(m2);
          t_main_plan.set_x_l(x);
          t_main_plan.set_seri_num(stripe_id % x);
          t_main_plan.set_block_size(block_size);
          t_main_plan.set_encodetype(m_encode_parameters.encodetype);
          if (m_encode_parameters.m_stripe_placementtype == ECProject::Horizontal)
            t_main_plan.set_rv_or_ch__isglobal(true);
          else
            t_main_plan.set_rv_or_ch__isglobal(false);
          t_main_plan.set_m_rack_id(t_map2rack);
          main_repair.push_back(t_main_plan);
          std::vector<proxy_proto::helpRepairPlan> t_help_plans;
          help_repair.push_back(t_help_plans);
          // update failed_map
          for (int jj = 0; jj < k2 + m2; jj++)
          {
            if (failed_map[jj][i])
            {
              failed_map[jj][i] = 0;
              failed_blocks_num -= 1;
              fb_row_cnt[jj] -= 1;
              fb_col_cnt[i] -= 1;
            }
          }
        }
      }
    }
    return true;
  }

  bool CoordinatorImpl::generate_repair_plan_for_single_block_lrc(
      std::vector<proxy_proto::mainRepairPlan> &main_repair,
      std::vector<std::vector<proxy_proto::helpRepairPlan>> &help_repair,
      int stripe_id, std::vector<Block *> &failed_blocks)
  {
    Stripe &t_stripe = m_stripe_table[stripe_id];
    int k = t_stripe.k;
    int g_m = t_stripe.g_m;
    int l = t_stripe.l;
    Block *t_block_ptr = failed_blocks[0];
    int block_size = t_block_ptr->block_size;
    int t_map2rack = t_block_ptr->map2rack;
    int t_map2group = t_block_ptr->map2group;
    int t_blockid = t_block_ptr->block_id;
    proxy_proto::mainRepairPlan t_main_plan;

    std::vector<Block *> blocks_map(k + g_m + l, nullptr);
    for (int i = 0; i < int(t_stripe.blocks.size()); i++)
    {
      int t_block_id = t_stripe.blocks[i]->block_id;
      blocks_map[t_block_id] = t_stripe.blocks[i];
    }
    int start_idx = 0;
    int stop_idx = 0;
    if (t_blockid >= k && t_blockid < k + g_m && !m_encode_parameters.partial_decoding)
    {
      start_idx = g_m - 1;
    }

    std::unordered_set<int> t_rack_set;
    int cnt = 0;
    if (t_blockid >= k && t_blockid < k + g_m) // global parity repaired by k data blocks (Azure-LRC as example)
    {
      for (int i = start_idx; i < k + g_m; i++)
      {
        if (i == t_blockid && t_blockid >= k)
        {
          t_main_plan.add_parity_blockids(t_blockid);
        }
        else if (i != t_blockid && cnt < k)
        {
          t_rack_set.insert(blocks_map[i]->map2rack);
          cnt++;
          if (cnt == k)
          {
            stop_idx = i;
          }
          if (i >= k)
          {
            t_main_plan.add_parity_blockids(i);
          }
        }
      }
    }
    else // other, repair locally
    {
      for (int i = 0; i < int(blocks_map.size()); i++)
      {
        if (blocks_map[i]->map2group == t_map2group && i != t_blockid)
        {
          t_rack_set.insert(blocks_map[i]->map2rack);
        }
        if (blocks_map[i]->map2group == t_map2group && i >= k)
        {
          t_main_plan.add_parity_blockids(i);
        }
      }
    }

    std::vector<proxy_proto::helpRepairPlan> t_help_plans;
    for (auto it = t_rack_set.begin(); it != t_rack_set.end(); it++)
    {
      int t_rack_id = *it;
      proxy_proto::helpRepairPlan t_help_plan;
      auto blocks_location = t_main_plan.add_racks();
      blocks_location->set_rack_id(t_rack_id);
      t_help_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
      t_help_plan.set_mainproxyip(m_rack_table[t_map2rack].proxy_ip);
      t_help_plan.set_mainproxyport(m_rack_table[t_map2rack].proxy_port + 1);
      t_help_plan.set_k(k);
      t_help_plan.set_m_g(g_m);
      t_help_plan.set_x_l(l);
      t_help_plan.set_block_size(block_size);
      t_help_plan.set_encodetype(m_encode_parameters.encodetype);
      if (t_blockid >= k && t_blockid < k + g_m)
      {
        t_help_plan.set_rv_or_ch__isglobal(true);
        for (int i = 0; i < int(t_main_plan.parity_blockids_size()); i++)
        {
          t_help_plan.add_parity_blockids(t_main_plan.parity_blockids(i));
        }
        // t_help_plan.add_parity_blockids(t_blockid);
      }
      else
      {
        t_help_plan.set_rv_or_ch__isglobal(false);
        t_help_plan.add_parity_blockids(t_main_plan.parity_blockids(0));
      }
      t_help_plan.set_failed_num(1);
      if (t_blockid >= k && t_blockid < k + g_m) // global parity
      {
        for (int i = start_idx; i < stop_idx; i++)
        {
          if (t_rack_id == blocks_map[i]->map2rack && i != t_blockid)
          {
            int t_node_id = blocks_map[i]->map2node;
            t_help_plan.add_blockkeys(blocks_map[i]->block_key);
            t_help_plan.add_blockids(blocks_map[i]->block_id);
            t_help_plan.add_datanodeip(m_node_table[t_node_id].node_ip);
            t_help_plan.add_datanodeport(m_node_table[t_node_id].node_port);

            blocks_location->add_blockkeys(blocks_map[i]->block_key);
            blocks_location->add_blockids(blocks_map[i]->map2col);
            blocks_location->add_datanodeip(m_node_table[t_node_id].node_ip);
            blocks_location->add_datanodeport(m_node_table[t_node_id].node_port);
          }
        }
      }
      else
      {
        for (int i = 0; i < int(blocks_map.size()); i++)
        {
          if (t_rack_id == blocks_map[i]->map2rack && blocks_map[i]->map2group == t_map2group && i != t_blockid)
          {
            int t_node_id = blocks_map[i]->map2node;
            t_help_plan.add_blockkeys(blocks_map[i]->block_key);
            t_help_plan.add_blockids(blocks_map[i]->block_id);
            t_help_plan.add_datanodeip(m_node_table[t_node_id].node_ip);
            t_help_plan.add_datanodeport(m_node_table[t_node_id].node_port);

            blocks_location->add_blockkeys(blocks_map[i]->block_key);
            blocks_location->add_blockids(blocks_map[i]->map2col);
            blocks_location->add_datanodeip(m_node_table[t_node_id].node_ip);
            blocks_location->add_datanodeport(m_node_table[t_node_id].node_port);
          }
        }
      }
      if (t_rack_id != t_map2rack)
        t_help_plans.push_back(t_help_plan);

      blocks_location->set_proxy_ip(m_rack_table[t_rack_id].proxy_ip);
      blocks_location->set_proxy_port(m_rack_table[t_rack_id].proxy_port);
    }
    t_main_plan.add_failed_blockkeys(t_block_ptr->block_key);
    t_main_plan.add_failed_blockids(t_block_ptr->block_id);
    t_main_plan.add_failed_datanodeip(m_node_table[t_block_ptr->map2node].node_ip);
    t_main_plan.add_failed_datanodeport(m_node_table[t_block_ptr->map2node].node_port);
    t_main_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
    t_main_plan.set_k(k);
    t_main_plan.set_m_g(g_m);
    t_main_plan.set_x_l(l);
    t_main_plan.set_block_size(block_size);
    t_main_plan.set_encodetype(m_encode_parameters.encodetype);
    if (t_blockid >= k && t_blockid < k + g_m)
      t_main_plan.set_rv_or_ch__isglobal(true);
    else
      t_main_plan.set_rv_or_ch__isglobal(false);
    t_main_plan.set_m_rack_id(t_map2rack);
    main_repair.push_back(t_main_plan);
    help_repair.push_back(t_help_plans);
    return true;
  }

  // Goal: repair blocks with fewest blocks
  bool CoordinatorImpl::generate_repair_plan_for_multi_blocks_lrc(
      std::vector<proxy_proto::mainRepairPlan> &main_repair,
      std::vector<std::vector<proxy_proto::helpRepairPlan>> &help_repair,
      int stripe_id, std::vector<Block *> &failed_blocks)
  {
    Stripe &t_stripe = m_stripe_table[stripe_id];
    int k = t_stripe.k;
    int g_m = t_stripe.g_m;
    int l = t_stripe.l;

    std::vector<int> failed_map(k + g_m + l, 0);
    std::vector<Block *> blocks_map(k + g_m + l, nullptr);
    std::vector<int> fb_group_cnt(l + 1, 0);
    int data_or_global_failed_num = 0;
    int failed_blocks_num = int(failed_blocks.size());
    for (int i = 0; i < failed_blocks_num; i++)
    {
      int block_id = failed_blocks[i]->block_id;
      int map2group = failed_blocks[i]->map2group;
      failed_map[block_id] = 1;
      fb_group_cnt[map2group] += 1;
      if (block_id < k + g_m)
      {
        data_or_global_failed_num += 1;
      }
    }
    for (int i = 0; i < int(t_stripe.blocks.size()); i++)
    {
      int t_block_id = t_stripe.blocks[i]->block_id;
      blocks_map[t_block_id] = t_stripe.blocks[i];
    }

    int iter_cnt = 0;
    while (failed_blocks_num > 0)
    {
      // repair in local group
      for (int group_id = 0; group_id < l; group_id++)
      {
        if (fb_group_cnt[group_id] == 1)
        {
          proxy_proto::mainRepairPlan t_main_plan;
          std::unordered_set<int> t_rack_set;
          Block *t_block_ptr = nullptr;
          for (int i = 0; i < int(blocks_map.size()); i++)
          {
            if (blocks_map[i]->map2group == group_id)
            {
              if (failed_map[i])
              {
                t_block_ptr = blocks_map[i];
              }
              else
              {
                t_rack_set.insert(blocks_map[i]->map2rack);
              }
              if (i >= k)
              {
                t_main_plan.add_parity_blockids(i);
              }
            }
          }

          int t_blockid = t_block_ptr->block_id;
          int t_map2rack = t_block_ptr->map2rack;
          int t_map2group = t_block_ptr->map2group;
          int block_size = t_block_ptr->block_size;
          std::vector<proxy_proto::helpRepairPlan> t_help_plans;
          for (auto it = t_rack_set.begin(); it != t_rack_set.end(); it++)
          {
            int t_rack_id = *it;
            proxy_proto::helpRepairPlan t_help_plan;
            auto blocks_location = t_main_plan.add_racks();
            blocks_location->set_rack_id(t_rack_id);
            t_help_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
            t_help_plan.set_mainproxyip(m_rack_table[t_map2rack].proxy_ip);
            t_help_plan.set_mainproxyport(m_rack_table[t_map2rack].proxy_port + 1);
            t_help_plan.set_k(k);
            t_help_plan.set_m_g(g_m);
            t_help_plan.set_x_l(l);
            t_help_plan.set_block_size(block_size);
            t_help_plan.set_encodetype(m_encode_parameters.encodetype);
            t_help_plan.set_rv_or_ch__isglobal(false);
            t_help_plan.set_failed_num(1);
            t_help_plan.add_parity_blockids(t_main_plan.parity_blockids(0));
            for (int i = 0; i < int(blocks_map.size()); i++)
            {
              if (t_rack_id == blocks_map[i]->map2rack && blocks_map[i]->map2group == t_map2group && !failed_map[i])
              {
                int t_node_id = blocks_map[i]->map2node;
                t_help_plan.add_blockkeys(blocks_map[i]->block_key);
                t_help_plan.add_blockids(blocks_map[i]->block_id);
                t_help_plan.add_datanodeip(m_node_table[t_node_id].node_ip);
                t_help_plan.add_datanodeport(m_node_table[t_node_id].node_port);

                blocks_location->add_blockkeys(blocks_map[i]->block_key);
                blocks_location->add_blockids(blocks_map[i]->map2col);
                blocks_location->add_datanodeip(m_node_table[t_node_id].node_ip);
                blocks_location->add_datanodeport(m_node_table[t_node_id].node_port);
              }
            }
            if (t_rack_id != t_map2rack)
              t_help_plans.push_back(t_help_plan);

            blocks_location->set_proxy_ip(m_rack_table[t_rack_id].proxy_ip);
            blocks_location->set_proxy_port(m_rack_table[t_rack_id].proxy_port);
          }
          t_main_plan.add_failed_blockkeys(t_block_ptr->block_key);
          t_main_plan.add_failed_blockids(t_block_ptr->block_id);
          t_main_plan.add_failed_datanodeip(m_node_table[t_block_ptr->map2node].node_ip);
          t_main_plan.add_failed_datanodeport(m_node_table[t_block_ptr->map2node].node_port);
          t_main_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
          t_main_plan.set_k(k);
          t_main_plan.set_m_g(g_m);
          t_main_plan.set_x_l(l);
          t_main_plan.set_block_size(block_size);
          t_main_plan.set_encodetype(m_encode_parameters.encodetype);
          t_main_plan.set_rv_or_ch__isglobal(false);
          t_main_plan.set_m_rack_id(t_map2rack);
          main_repair.push_back(t_main_plan);
          help_repair.push_back(t_help_plans);
          // update
          failed_map[t_blockid] = 0;
          fb_group_cnt[group_id] = 0;
          failed_blocks_num -= 1;
          if (t_blockid < k)
          {
            data_or_global_failed_num -= 1;
          }
        }
      }
      if (data_or_global_failed_num > 0 && data_or_global_failed_num <= g_m)
      {
        int t_main_rack_id = -1;
        int block_size = 0;
        int cnt = 0;
        int start_idx = 0;
        int stop_idx = 0;

        // for single-block
        for (int j = 0; j < k + g_m; j++)
        {
          if (failed_map[j])
          {
            if (data_or_global_failed_num == 1 && j >= k && !m_encode_parameters.partial_decoding)
            {
              start_idx = g_m - 1;
            }
            break;
          }
        }

        proxy_proto::mainRepairPlan t_main_plan;
        std::unordered_set<int> t_rack_set;
        for (int i = start_idx; i < k + g_m; i++)
        {
          if (failed_map[i])
          {
            Block *t_block_ptr = blocks_map[i];
            t_main_plan.add_failed_blockkeys(t_block_ptr->block_key);
            t_main_plan.add_failed_blockids(t_block_ptr->block_id);
            t_main_plan.add_failed_datanodeip(m_node_table[t_block_ptr->map2node].node_ip);
            t_main_plan.add_failed_datanodeport(m_node_table[t_block_ptr->map2node].node_port);
            t_main_rack_id = t_block_ptr->map2rack;
            block_size = t_block_ptr->block_size;
          }
          else if (cnt < k)
          {
            t_rack_set.insert(blocks_map[i]->map2rack);
            cnt++;
            if (cnt == k)
            {
              stop_idx = i;
            }
            if (i >= k && i < k + g_m) // surviving parities for repair
            {
              t_main_plan.add_parity_blockids(i);
            }
          }
          if (i >= k && failed_map[i]) // failed parities to be repaired
          {
            t_main_plan.add_parity_blockids(i);
          }
        }

        std::vector<proxy_proto::helpRepairPlan> t_help_plans;
        for (auto it = t_rack_set.begin(); it != t_rack_set.end(); it++)
        {
          int t_rack_id = *it;
          proxy_proto::helpRepairPlan t_help_plan;
          auto blocks_location = t_main_plan.add_racks();
          blocks_location->set_rack_id(t_rack_id);
          t_help_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
          t_help_plan.set_mainproxyip(m_rack_table[t_main_rack_id].proxy_ip);
          t_help_plan.set_mainproxyport(m_rack_table[t_main_rack_id].proxy_port + 1);
          t_help_plan.set_k(k);
          t_help_plan.set_m_g(g_m);
          t_help_plan.set_x_l(l);
          t_help_plan.set_block_size(block_size);
          t_help_plan.set_encodetype(m_encode_parameters.encodetype);
          t_help_plan.set_rv_or_ch__isglobal(true);
          t_help_plan.set_failed_num(data_or_global_failed_num);
          for (int i = start_idx; i <= stop_idx; i++)
          {
            if (t_rack_id == blocks_map[i]->map2rack && !failed_map[i])
            {
              int t_node_id = blocks_map[i]->map2node;
              t_help_plan.add_blockkeys(blocks_map[i]->block_key);
              t_help_plan.add_blockids(blocks_map[i]->block_id);
              t_help_plan.add_datanodeip(m_node_table[t_node_id].node_ip);
              t_help_plan.add_datanodeport(m_node_table[t_node_id].node_port);

              blocks_location->add_blockkeys(blocks_map[i]->block_key);
              blocks_location->add_blockids(blocks_map[i]->map2col);
              blocks_location->add_datanodeip(m_node_table[t_node_id].node_ip);
              blocks_location->add_datanodeport(m_node_table[t_node_id].node_port);
            }
          }
          for (int i = 0; i < int(t_main_plan.parity_blockids_size()); i++)
          {
            t_help_plan.add_parity_blockids(t_main_plan.parity_blockids(i));
          }
          if (t_rack_id != t_main_rack_id)
            t_help_plans.push_back(t_help_plan);

          blocks_location->set_proxy_ip(m_rack_table[t_rack_id].proxy_ip);
          blocks_location->set_proxy_port(m_rack_table[t_rack_id].proxy_port);
        }
        t_main_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
        t_main_plan.set_k(k);
        t_main_plan.set_m_g(g_m);
        t_main_plan.set_x_l(l);
        t_main_plan.set_block_size(block_size);
        t_main_plan.set_encodetype(m_encode_parameters.encodetype);
        t_main_plan.set_rv_or_ch__isglobal(true);
        t_main_plan.set_m_rack_id(t_main_rack_id);
        main_repair.push_back(t_main_plan);
        help_repair.push_back(t_help_plans);

        // update
        for (int i = 0; i < k + g_m; i++)
        {
          if (failed_map[i])
          {
            failed_map[i] = 0;
            failed_blocks_num -= 1;
            int group_id = blocks_map[i]->map2group;
            fb_group_cnt[group_id] -= 1;
          }
        }
        data_or_global_failed_num = 0;
      }

      if (iter_cnt > 0 && failed_blocks_num > 0 && failed_blocks_num <= g_m + 1)
      {
        bool partial_decoding = m_encode_parameters.partial_decoding;
        if (failed_blocks_num > g_m)
        {
          partial_decoding = false;
        }
        int t_main_rack_id = -1;
        int block_size = 0;
        int max_group_id = -1;
        int max_group_num = 0;
        for (int i = 0; i < l; i++)
        {
          if (max_group_num < fb_group_cnt[i])
          {
            max_group_id = i;
            max_group_num = fb_group_cnt[i];
          }
        }
        proxy_proto::mainRepairPlan t_main_plan;
        std::unordered_set<int> t_rack_set;
        for (int i = 0; i < int(blocks_map.size()); i++)
        {
          if (failed_map[i])
          {
            Block *t_block_ptr = blocks_map[i];
            t_main_plan.add_failed_blockkeys(t_block_ptr->block_key);
            t_main_plan.add_failed_blockids(t_block_ptr->block_id);
            t_main_plan.add_failed_datanodeip(m_node_table[t_block_ptr->map2node].node_ip);
            t_main_plan.add_failed_datanodeport(m_node_table[t_block_ptr->map2node].node_port);
            t_main_rack_id = t_block_ptr->map2rack;
            block_size = t_block_ptr->block_size;
          }
          else
          {
            t_rack_set.insert(blocks_map[i]->map2rack);
          }
          if (i >= k && i < k + g_m)
          {
            t_main_plan.add_parity_blockids(i);
          }
          else if (i >= k + g_m && blocks_map[i]->map2group == max_group_id)
          {
            t_main_plan.add_parity_blockids(i);
          }
        }

        std::vector<proxy_proto::helpRepairPlan> t_help_plans;
        for (auto it = t_rack_set.begin(); it != t_rack_set.end(); it++)
        {
          int t_rack_id = *it;
          proxy_proto::helpRepairPlan t_help_plan;
          auto blocks_location = t_main_plan.add_racks();
          blocks_location->set_rack_id(t_rack_id);
          t_help_plan.set_if_partial_decoding(partial_decoding);
          t_help_plan.set_mainproxyip(m_rack_table[t_main_rack_id].proxy_ip);
          t_help_plan.set_mainproxyport(m_rack_table[t_main_rack_id].proxy_port + 1);
          t_help_plan.set_k(k);
          t_help_plan.set_m_g(g_m);
          t_help_plan.set_x_l(l);
          t_help_plan.set_block_size(block_size);
          t_help_plan.set_encodetype(m_encode_parameters.encodetype);
          t_help_plan.set_rv_or_ch__isglobal(true);
          t_help_plan.set_failed_num(data_or_global_failed_num);
          for (int i = 0; i < k + g_m + l; i++)
          {
            if (t_rack_id == blocks_map[i]->map2rack && !failed_map[i])
            {
              int t_node_id = blocks_map[i]->map2node;
              t_help_plan.add_blockkeys(blocks_map[i]->block_key);
              t_help_plan.add_blockids(blocks_map[i]->block_id);
              t_help_plan.add_datanodeip(m_node_table[t_node_id].node_ip);
              t_help_plan.add_datanodeport(m_node_table[t_node_id].node_port);

              blocks_location->add_blockkeys(blocks_map[i]->block_key);
              blocks_location->add_blockids(blocks_map[i]->map2col);
              blocks_location->add_datanodeip(m_node_table[t_node_id].node_ip);
              blocks_location->add_datanodeport(m_node_table[t_node_id].node_port);
            }
          }
          for (int i = 0; i < int(t_main_plan.parity_blockids_size()); i++)
          {
            t_help_plan.add_parity_blockids(t_main_plan.parity_blockids(i));
          }
          if (t_rack_id != t_main_rack_id)
            t_help_plans.push_back(t_help_plan);

          blocks_location->set_proxy_ip(m_rack_table[t_rack_id].proxy_ip);
          blocks_location->set_proxy_port(m_rack_table[t_rack_id].proxy_port);
        }
        t_main_plan.set_if_partial_decoding(partial_decoding);
        t_main_plan.set_k(k);
        t_main_plan.set_m_g(g_m);
        t_main_plan.set_x_l(l);
        t_main_plan.set_block_size(block_size);
        t_main_plan.set_encodetype(m_encode_parameters.encodetype);
        t_main_plan.set_rv_or_ch__isglobal(true);
        t_main_plan.set_m_rack_id(t_main_rack_id);
        main_repair.push_back(t_main_plan);
        help_repair.push_back(t_help_plans);

        // update
        for (int i = 0; i < k + g_m + l; i++)
        {
          if (failed_map[i])
          {
            failed_map[i] = 0;
            failed_blocks_num -= 1;
            int group_id = blocks_map[i]->map2group;
            fb_group_cnt[group_id] -= 1;
          }
        }
      }
      else if (iter_cnt > 0 && failed_blocks_num > g_m + 1)
      {
        std::cout << "Undecodable!!!" << std::endl;
        return false;
      }
      iter_cnt++;
    }

    return true;
  }

  bool CoordinatorImpl::generate_repair_plan_for_rs(
      std::vector<proxy_proto::mainRepairPlan> &main_repair,
      std::vector<std::vector<proxy_proto::helpRepairPlan>> &help_repair,
      int stripe_id, std::vector<Block *> &failed_blocks)
  {
    Stripe &t_stripe = m_stripe_table[stripe_id];
    int k = t_stripe.k;
    int g_m = t_stripe.g_m;
    std::vector<int> failed_map(k + g_m, 0);
    std::vector<Block *> blocks_map(k + g_m, nullptr);
    int failed_blocks_num = int(failed_blocks.size());
    for (int i = 0; i < failed_blocks_num; i++)
    {
      int block_id = failed_blocks[i]->block_id;
      // int map2group = failed_blocks[i]->map2group;
      failed_map[block_id] = 1;
    }
    for (int i = 0; i < int(t_stripe.blocks.size()); i++)
    {
      int t_block_id = t_stripe.blocks[i]->block_id;
      blocks_map[t_block_id] = t_stripe.blocks[i];
    }
    if (failed_blocks_num > g_m)
    {
      std::cout << "Undecodable!!!" << std::endl;
      return false;
    }
    else
    {
      int t_main_rack_id = -1;
      int block_size = 0;
      int cnt = 0;
      int start_idx = 0;
      int stop_idx = 0;

      // for single-block
      for (int j = 0; j < k + g_m; j++)
      {
        if (failed_map[j])
        {
          if (failed_blocks_num == 1 && j >= (k + g_m) - ((k + g_m) % g_m) && !m_encode_parameters.partial_decoding)
          {
            start_idx = g_m - 1;
          }
          break;
        }
      }

      proxy_proto::mainRepairPlan t_main_plan;
      std::unordered_set<int> t_rack_set;
      for (int i = start_idx; i < k + g_m; i++)
      {
        if (failed_map[i])
        {
          Block *t_block_ptr = blocks_map[i];
          t_main_plan.add_failed_blockkeys(t_block_ptr->block_key);
          t_main_plan.add_failed_blockids(t_block_ptr->block_id);
          t_main_plan.add_failed_datanodeip(m_node_table[t_block_ptr->map2node].node_ip);
          t_main_plan.add_failed_datanodeport(m_node_table[t_block_ptr->map2node].node_port);
          t_main_rack_id = t_block_ptr->map2rack;
          block_size = t_block_ptr->block_size;
        }
        else if (cnt < k)
        {
          t_rack_set.insert(blocks_map[i]->map2rack);
          cnt++;
          if (cnt == k)
          {
            stop_idx = i;
          }
          if (i >= k) // surviving parities for repair
          {
            t_main_plan.add_parity_blockids(i);
          }
        }
        if (i >= k && failed_map[i]) // failed parities to be repaired
        {
          t_main_plan.add_parity_blockids(i);
        }
      }

      std::vector<proxy_proto::helpRepairPlan> t_help_plans;
      for (auto it = t_rack_set.begin(); it != t_rack_set.end(); it++)
      {
        int t_rack_id = *it;
        proxy_proto::helpRepairPlan t_help_plan;
        auto blocks_location = t_main_plan.add_racks();
        blocks_location->set_rack_id(t_rack_id);
        t_help_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
        t_help_plan.set_mainproxyip(m_rack_table[t_main_rack_id].proxy_ip);
        t_help_plan.set_mainproxyport(m_rack_table[t_main_rack_id].proxy_port + 1);
        t_help_plan.set_k(k);
        t_help_plan.set_m_g(g_m);
        t_help_plan.set_x_l(k / g_m);
        t_help_plan.set_rv_or_ch__isglobal(true);
        t_help_plan.set_block_size(block_size);
        t_help_plan.set_encodetype(m_encode_parameters.encodetype);
        t_help_plan.set_failed_num(failed_blocks_num);
        for (int i = start_idx; i <= stop_idx; i++)
        {
          if (t_rack_id == blocks_map[i]->map2rack && !failed_map[i]) // to retrieve
          {
            int t_node_id = blocks_map[i]->map2node;
            t_help_plan.add_blockkeys(blocks_map[i]->block_key);
            t_help_plan.add_blockids(blocks_map[i]->block_id);
            t_help_plan.add_datanodeip(m_node_table[t_node_id].node_ip);
            t_help_plan.add_datanodeport(m_node_table[t_node_id].node_port);

            blocks_location->add_blockkeys(blocks_map[i]->block_key);
            blocks_location->add_blockids(blocks_map[i]->map2col);
            blocks_location->add_datanodeip(m_node_table[t_node_id].node_ip);
            blocks_location->add_datanodeport(m_node_table[t_node_id].node_port);
          }
        }
        for (int i = 0; i < int(t_main_plan.parity_blockids_size()); i++)
        {
          t_help_plan.add_parity_blockids(t_main_plan.parity_blockids(i));
        }
        if (t_rack_id != t_main_rack_id)
          t_help_plans.push_back(t_help_plan);

        blocks_location->set_proxy_ip(m_rack_table[t_rack_id].proxy_ip);
        blocks_location->set_proxy_port(m_rack_table[t_rack_id].proxy_port);
      }
      t_main_plan.set_if_partial_decoding(m_encode_parameters.partial_decoding);
      t_main_plan.set_k(k);
      t_main_plan.set_m_g(g_m);
      t_main_plan.set_x_l(k / g_m);
      t_main_plan.set_rv_or_ch__isglobal(true);
      t_main_plan.set_block_size(block_size);
      t_main_plan.set_encodetype(m_encode_parameters.encodetype);
      t_main_plan.set_m_rack_id(t_main_rack_id);
      main_repair.push_back(t_main_plan);
      help_repair.push_back(t_help_plans);

      // update
      for (int i = 0; i < k + g_m; i++)
      {
        if (failed_map[i])
        {
          failed_map[i] = 0;
          failed_blocks_num -= 1;
        }
      }
    }
    return true;
  }

  void CoordinatorImpl::simulation_repair(
      std::vector<proxy_proto::mainRepairPlan> &main_repair,
      int &cross_rack_num)
  {
    for (int i = 0; i < int(main_repair.size()); i++)
    {
      int failed_block_num = int(main_repair[i].failed_blockkeys_size());
      int main_rack_id = main_repair[i].m_rack_id();
      for (int j = 0; j < int(main_repair[i].racks_size()); j++)
      {
        int t_rack_id = main_repair[i].racks(j).rack_id();
        if (main_rack_id != t_rack_id) // cross-rack
        {
          int help_block_num = int(main_repair[i].racks(j).blockkeys_size());
          if (help_block_num > failed_block_num && m_encode_parameters.partial_decoding)
          {
            cross_rack_num += failed_block_num;
          }
          else
          {
            cross_rack_num += help_block_num;
          }
        }
      }
    }
  }

  bool CoordinatorImpl::generate_repair_plan_for_single_block_mlec(std::vector<proxy_proto::mainRepairPlan> &main_repair,
                                                                   std::vector<std::vector<proxy_proto::helpRepairPlan>> &help_repair, int stripe_id, std::vector<Block *> &failed_blocks)
  {
    return generate_repair_plan_for_single_block_hpc(main_repair, help_repair, stripe_id, failed_blocks);
  }

  bool CoordinatorImpl::generate_repair_plan_for_rand_multi_blocks_mlec(std::vector<proxy_proto::mainRepairPlan> &main_repair, std::vector<std::vector<proxy_proto::helpRepairPlan>> &help_repair, int stripe_id, std::vector<Block *> &failed_blocks)
  {

    return generate_repair_plan_for_rand_multi_blocks_hpc(main_repair, help_repair, stripe_id, failed_blocks);
  }

}