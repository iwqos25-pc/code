#include "coordinator.h"
#include "tinyxml2.h"
#include <random>
#include <algorithm>
#include <unistd.h>
#include "lrc.h"
#include "pc.h"
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

namespace ECProject
{
  grpc::Status CoordinatorImpl::setParameter(
      grpc::ServerContext *context,
      const coordinator_proto::Parameter *parameter,
      coordinator_proto::RepIfSetParaSuccess *setParameterReply)
  {
    ECProject::EncodeType encode_type = (ECProject::EncodeType)parameter->encodetype();
    if (encode_type == ECProject::HPC || encode_type == ECProject::MLEC)
    {
      ECSchema system_metadata(parameter->partial_decoding(),
                               parameter->approach(),
                               (ECProject::EncodeType)parameter->encodetype(),
                               (ECProject::SingleStripePlacementType)parameter->s_stripe_placementtype(),
                               (ECProject::MultiStripesPlacementType)parameter->m_stripe_placementtype(),
                               parameter->k1_col_datablock(),
                               parameter->m1_col_parityblock(),
                               parameter->k2_row_datablock(),
                               parameter->m2_row_parityblock(),
                               parameter->x_stripepermergegroup());
      m_encode_parameters = system_metadata;
    }
    else
    {
      ECSchema system_metadata(parameter->partial_decoding(),
                               parameter->approach(),
                               (ECProject::EncodeType)parameter->encodetype(),
                               (ECProject::SingleStripePlacementType)parameter->s_stripe_placementtype(),
                               (ECProject::MultiStripesPlacementType)parameter->m_stripe_placementtype(),
                               parameter->k_datablock(),
                               parameter->l_localparityblock(),
                               parameter->g_m_globalparityblock(),
                               parameter->x_stripepermergegroup());
      m_encode_parameters = system_metadata;
    }
    setParameterReply->set_ifsetparameter(true);
    m_cur_rack_id = 0;
    m_cur_stripe_id = 0;
    m_object_commit_table.clear();
    m_object_updating_table.clear();
    m_stripe_deleting_table.clear();
    for (auto it = m_rack_table.begin(); it != m_rack_table.end(); it++)
    {
      Rack &t_rack = it->second;
      t_rack.stripes.clear();
    }
    for (auto it = m_node_table.begin(); it != m_node_table.end(); it++)
    {
      Node &t_node = it->second;
      t_node.stripes.clear();
    }
    m_stripe_table.clear();
    m_m1cols2rack.clear();
    m_col2rack.clear();
    m_merge_groups.clear();
    m_free_racks.clear();
    m_merge_degree = 0;
    m_agg_start_rid = 0;
    std::cout << "setParameter success" << std::endl;
    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::sayHelloToCoordinator(
      grpc::ServerContext *context,
      const coordinator_proto::RequestToCoordinator *helloRequestToCoordinator,
      coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator)
  {
    std::string prefix("Hello ");
    helloReplyFromCoordinator->set_message(prefix + helloRequestToCoordinator->name());
    std::cout << prefix + helloRequestToCoordinator->name() << std::endl;
    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::uploadOriginKeyValue(
      grpc::ServerContext *context,
      const coordinator_proto::RequestProxyIPPort *keyValueSize,
      coordinator_proto::ReplyProxyIPPort *proxyIPPort)
  {
    gettimeofday(&start_time, NULL);
    std::string key = keyValueSize->key();
    m_mutex.lock();
    m_object_commit_table.erase(key);
    m_mutex.unlock();
    int valuesizebytes = keyValueSize->valuesizebytes();

    ObjectInfo new_object;

    int k = m_encode_parameters.k_datablock;
    int g_m = m_encode_parameters.g_m_globalparityblock;
    int l = m_encode_parameters.l_localparityblock;
    int k1 = m_encode_parameters.k1_col_datablock;
    int m1 = m_encode_parameters.m1_col_parityblock;
    int k2 = m_encode_parameters.k2_row_datablock;
    int m2 = m_encode_parameters.m2_row_parityblock;
    int x = m_encode_parameters.x_stripepermergegroup;
    ECProject::MultiStripesPlacementType m_mul_placement_type = m_encode_parameters.m_stripe_placementtype;

    new_object.object_size = valuesizebytes;
    int block_size = ceil(valuesizebytes, k);

    proxy_proto::ObjectAndPlacement object_placement;
    object_placement.set_key(key);
    object_placement.set_valuesizebyte(valuesizebytes);
    object_placement.set_k(k);
    object_placement.set_g_m(g_m);
    object_placement.set_l(l);
    object_placement.set_encode_type(m_encode_parameters.encodetype);
    object_placement.set_block_size(block_size);
    object_placement.set_k1(k1);
    object_placement.set_m1(m1);
    object_placement.set_k2(k2);
    object_placement.set_m2(m2);
    object_placement.set_x(x);
    if (m_mul_placement_type == ECProject::Vertical)
    {
      object_placement.set_isvertical(true);
    }
    else
    {
      object_placement.set_isvertical(false);
    }

    Stripe t_stripe;
    t_stripe.stripe_id = m_cur_stripe_id++;
    t_stripe.k = k;
    t_stripe.l = l;
    t_stripe.g_m = g_m;
    t_stripe.k1 = k1;
    t_stripe.m1 = m1;
    t_stripe.k2 = k2;
    t_stripe.m2 = m2;
    t_stripe.object_keys.push_back(key);
    // t_stripe.object_sizes.push_back(valuesizebytes);
    m_stripe_table[t_stripe.stripe_id] = t_stripe;
    new_object.map2stripe = t_stripe.stripe_id;

    object_placement.set_seri_num(t_stripe.stripe_id % x);
    int s_rack_id = -1;

    if (m_encode_parameters.encodetype == ECProject::HPC)
    {
      s_rack_id = generate_placement_for_hpc(t_stripe.stripe_id, block_size);
    }
    else if (m_encode_parameters.encodetype == ECProject::MLEC)
    {
      s_rack_id = generate_placement_for_mlec(t_stripe.stripe_id, block_size);
    }
    else if (m_encode_parameters.encodetype == ECProject::Azure_LRC)
    {
      s_rack_id = generate_placement_for_lrc(t_stripe.stripe_id, block_size);
    }
    else
    {
      s_rack_id = generate_placement_for_rs(t_stripe.stripe_id, block_size);
    }

    Stripe &stripe = m_stripe_table[t_stripe.stripe_id];
    object_placement.set_stripe_id(stripe.stripe_id);
    for (int i = 0; i < int(stripe.blocks.size()); i++)
    {
      object_placement.add_datanodeip(m_node_table[stripe.blocks[i]->map2node].node_ip);
      object_placement.add_datanodeport(m_node_table[stripe.blocks[i]->map2node].node_port);
      object_placement.add_blockkeys(stripe.blocks[i]->block_key);
    }

    grpc::ClientContext cont;
    proxy_proto::SetReply set_reply;
    std::string selected_proxy_ip = m_rack_table[s_rack_id].proxy_ip;
    int selected_proxy_port = m_rack_table[s_rack_id].proxy_port;
    std::string chosen_proxy = selected_proxy_ip + ":" + std::to_string(selected_proxy_port);
    grpc::Status status = m_proxy_ptrs[chosen_proxy]->encodeAndSetObject(&cont, object_placement, &set_reply);
    proxyIPPort->set_proxyip(selected_proxy_ip);
    proxyIPPort->set_proxyport(selected_proxy_port + 1); // use another port to accept data
    if (status.ok())
    {
      m_mutex.lock();
      m_object_updating_table[key] = new_object;
      m_mutex.unlock();
    }
    else
    {
      std::cout << "[SET] Send object placement failed!" << std::endl;
    }
    gettimeofday(&end_time, NULL);
    m_set_time = end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::getValue(
      grpc::ServerContext *context,
      const coordinator_proto::KeyAndClientIP *keyClient,
      coordinator_proto::RepIfGetSuccess *getReplyClient)
  {
    try
    {
      std::string key = keyClient->key();
      std::string client_ip = keyClient->clientip();
      int client_port = keyClient->clientport();
      ObjectInfo object_info;
      m_mutex.lock();
      object_info = m_object_commit_table.at(key);
      m_mutex.unlock();
      int k = m_encode_parameters.k_datablock;
      int g_m = m_encode_parameters.g_m_globalparityblock;
      int l = m_encode_parameters.l_localparityblock;
      int k1 = m_encode_parameters.k1_col_datablock;
      int m1 = m_encode_parameters.m1_col_parityblock;
      int k2 = m_encode_parameters.k2_row_datablock;
      int m2 = m_encode_parameters.m2_row_parityblock;
      int x = m_encode_parameters.x_stripepermergegroup;

      grpc::ClientContext decode_and_get;
      proxy_proto::ObjectAndPlacement object_placement;
      grpc::Status status;
      proxy_proto::GetReply get_reply;
      getReplyClient->set_valuesizebytes(object_info.object_size);
      object_placement.set_key(key);
      object_placement.set_valuesizebyte(object_info.object_size);
      object_placement.set_k(k);
      object_placement.set_l(l);
      object_placement.set_g_m(g_m);
      object_placement.set_k1(k1);
      object_placement.set_m1(m1);
      object_placement.set_k2(k2);
      object_placement.set_m2(m2);
      object_placement.set_x(x);
      object_placement.set_stripe_id(object_info.map2stripe);
      object_placement.set_encode_type(m_encode_parameters.encodetype);
      object_placement.set_clientip(client_ip);
      object_placement.set_clientport(client_port);
      Stripe &t_stripe = m_stripe_table[object_info.map2stripe];
      std::unordered_set<int> t_rack_set;
      for (int i = 0; i < int(t_stripe.blocks.size()); i++)
      {
        if (t_stripe.blocks[i]->map2key == key)
        {
          object_placement.add_datanodeip(m_node_table[t_stripe.blocks[i]->map2node].node_ip);
          object_placement.add_datanodeport(m_node_table[t_stripe.blocks[i]->map2node].node_port);
          object_placement.add_blockkeys(t_stripe.blocks[i]->block_key);
          object_placement.add_blockids(t_stripe.blocks[i]->block_id);
          t_rack_set.insert(t_stripe.blocks[i]->map2rack);
        }
      }
      // randomly select a rack
      int idx = rand_num(int(t_rack_set.size()));
      int r_rack_id = *(std::next(t_rack_set.begin(), idx));
      std::string chosen_proxy = m_rack_table[r_rack_id].proxy_ip + ":" + std::to_string(m_rack_table[r_rack_id].proxy_port);
      status = m_proxy_ptrs[chosen_proxy]->decodeAndGetObject(&decode_and_get, object_placement, &get_reply);
      if (status.ok())
      {
        std::cout << "[GET] getting value of " << key << std::endl;
      }
    }
    catch (std::exception &e)
    {
      std::cout << "getValue exception" << std::endl;
      std::cout << e.what() << std::endl;
    }
    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::delByKey(
      grpc::ServerContext *context,
      const coordinator_proto::KeyFromClient *del_key,
      coordinator_proto::RepIfDeling *delReplyClient)
  {
    try
    {
      std::string key = del_key->key();
      ObjectInfo object_info;
      m_mutex.lock();
      object_info = m_object_commit_table.at(key);
      m_object_updating_table[key] = m_object_commit_table[key];
      m_mutex.unlock();

      grpc::ClientContext context;
      proxy_proto::NodeAndBlock node_block;
      grpc::Status status;
      proxy_proto::DelReply del_reply;
      Stripe &t_stripe = m_stripe_table[object_info.map2stripe];
      std::unordered_set<int> t_rack_set;
      for (int i = 0; i < int(t_stripe.blocks.size()); i++)
      {
        if (t_stripe.blocks[i]->map2key == key)
        {
          node_block.add_datanodeip(m_node_table[t_stripe.blocks[i]->map2node].node_ip);
          node_block.add_datanodeport(m_node_table[t_stripe.blocks[i]->map2node].node_port);
          node_block.add_blockkeys(t_stripe.blocks[i]->block_key);
          t_rack_set.insert(t_stripe.blocks[i]->map2rack);
        }
      }
      node_block.set_stripe_id(-1); // as a flag to distinguish delete key or stripe
      node_block.set_key(key);
      // randomly select a rack
      int idx = rand_num(int(t_rack_set.size()));
      int r_rack_id = *(std::next(t_rack_set.begin(), idx));
      std::string chosen_proxy = m_rack_table[r_rack_id].proxy_ip + ":" + std::to_string(m_rack_table[r_rack_id].proxy_port);
      status = m_proxy_ptrs[chosen_proxy]->deleteBlock(&context, node_block, &del_reply);
      delReplyClient->set_ifdeling(true);
      if (status.ok())
      {
        std::cout << "[DEL] deleting value of " << key << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cout << "deleteByKey exception" << std::endl;
      std::cout << e.what() << std::endl;
    }
    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::delByStripe(
      grpc::ServerContext *context,
      const coordinator_proto::StripeIdFromClient *stripeid,
      coordinator_proto::RepIfDeling *delReplyClient)
  {
    try
    {
      int t_stripe_id = stripeid->stripe_id();
      m_mutex.lock();
      m_stripe_deleting_table.push_back(t_stripe_id);
      m_mutex.unlock();

      grpc::ClientContext context;
      proxy_proto::NodeAndBlock node_block;
      grpc::Status status;
      proxy_proto::DelReply del_reply;
      Stripe &t_stripe = m_stripe_table[t_stripe_id];
      std::unordered_set<int> t_rack_set;
      for (int i = 0; i < int(t_stripe.blocks.size()); i++)
      {
        if (t_stripe.blocks[i]->map2stripe == t_stripe_id)
        {
          node_block.add_datanodeip(m_node_table[t_stripe.blocks[i]->map2node].node_ip);
          node_block.add_datanodeport(m_node_table[t_stripe.blocks[i]->map2node].node_port);
          node_block.add_blockkeys(t_stripe.blocks[i]->block_key);
          t_rack_set.insert(t_stripe.blocks[i]->map2rack);
        }
      }
      node_block.set_stripe_id(t_stripe_id);
      node_block.set_key("");
      // randomly select a rack
      int idx = rand_num(int(t_rack_set.size()));
      int r_rack_id = *(std::next(t_rack_set.begin(), idx));
      std::string chosen_proxy = m_rack_table[r_rack_id].proxy_ip + ":" + std::to_string(m_rack_table[r_rack_id].proxy_port);
      status = m_proxy_ptrs[chosen_proxy]->deleteBlock(&context, node_block, &del_reply);
      delReplyClient->set_ifdeling(true);
      if (status.ok())
      {
        std::cout << "[DEL] deleting value of Stripe " << t_stripe_id << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cout << "deleteByStripe exception" << std::endl;
      std::cout << e.what() << std::endl;
    }
    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::listStripes(
      grpc::ServerContext *context,
      const coordinator_proto::RequestToCoordinator *req,
      coordinator_proto::RepStripeIds *listReplyClient)
  {
    try
    {
      for (auto it = m_stripe_table.begin(); it != m_stripe_table.end(); it++)
      {
        listReplyClient->add_stripe_ids(it->first);
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::checkalive(
      grpc::ServerContext *context,
      const coordinator_proto::RequestToCoordinator *helloRequestToCoordinator,
      coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator)
  {

    std::cout << "[Coordinator Check] alive " << helloRequestToCoordinator->name() << std::endl;
    return grpc::Status::OK;
  }
  grpc::Status CoordinatorImpl::reportCommitAbort(
      grpc::ServerContext *context,
      const coordinator_proto::CommitAbortKey *commit_abortkey,
      coordinator_proto::ReplyFromCoordinator *helloReplyFromCoordinator)
  {
    std::string key = commit_abortkey->key();
    ECProject::OpperateType opp = (ECProject::OpperateType)commit_abortkey->opp();
    int stripe_id = commit_abortkey->stripe_id();
    std::unique_lock<std::mutex> lck(m_mutex);
    try
    {
      if (commit_abortkey->ifcommitmetadata())
      {
        if (opp == SET)
        {
          m_object_commit_table[key] = m_object_updating_table[key];
          cv.notify_all();
          m_object_updating_table.erase(key);
        }
        else if (opp == DEL) // delete the metadata
        {
          if (stripe_id < 0) // delete key
          {
            if (IF_DEBUG)
            {
              std::cout << "[DEL] Proxy report delete key finish!" << std::endl;
            }
            ObjectInfo object_info = m_object_commit_table.at(key);
            stripe_id = object_info.map2stripe;
            m_object_commit_table.erase(key); // update commit table
            cv.notify_all();
            m_object_updating_table.erase(key);
            Stripe &t_stripe = m_stripe_table[stripe_id];
            std::vector<Block *>::iterator it1;
            for (it1 = t_stripe.blocks.begin(); it1 != t_stripe.blocks.end();)
            {
              if ((*it1)->map2key == key)
              {
                it1 = t_stripe.blocks.erase(it1);
              }
              else
              {
                it1++;
              }
            }
            if (t_stripe.blocks.empty()) // update tables
            {
              update_tables_when_rm_stripe(stripe_id);
            }
            std::map<int, Rack>::iterator it2; // update rack table
            for (it2 = m_rack_table.begin(); it2 != m_rack_table.end(); it2++)
            {
              Rack &t_rack = it2->second;
              for (it1 = t_rack.blocks.begin(); it1 != t_rack.blocks.end();)
              {
                if ((*it1)->map2key == key)
                {
                  update_stripe_info_in_node(false, (*it1)->map2node, (*it1)->map2stripe); // update node table
                  it1 = t_rack.blocks.erase(it1);
                }
                else
                {
                  it1++;
                }
              }
            }
          } // delete stripe
          else
          {
            if (IF_DEBUG)
            {
              std::cout << "[DEL] Proxy report delete stripe finish!" << std::endl;
            }
            auto its = std::find(m_stripe_deleting_table.begin(), m_stripe_deleting_table.end(), stripe_id);
            if (its != m_stripe_deleting_table.end())
            {
              m_stripe_deleting_table.erase(its);
            }
            cv.notify_all();
            std::unordered_set<std::string> object_keys_set;
            // update rack table
            std::map<int, Rack>::iterator it2;
            for (it2 = m_rack_table.begin(); it2 != m_rack_table.end(); it2++)
            {
              Rack &t_rack = it2->second;
              for (auto it1 = t_rack.blocks.begin(); it1 != t_rack.blocks.end();)
              {
                if ((*it1)->map2stripe == stripe_id)
                {
                  object_keys_set.insert((*it1)->map2key);
                  it1 = t_rack.blocks.erase(it1);
                }
                else
                {
                  it1++;
                }
              }
            }
            // update tables
            update_tables_when_rm_stripe(stripe_id);
            // update commit table
            for (auto it5 = object_keys_set.begin(); it5 != object_keys_set.end(); it5++)
            {
              auto it6 = m_object_commit_table.find(*it5);
              if (it6 != m_object_commit_table.end())
              {
                m_object_commit_table.erase(it6);
              }
            }
            // merge group
          }
        }
      }
      else
      {
        m_object_updating_table.erase(key);
      }
    }
    catch (std::exception &e)
    {
      std::cout << "reportCommitAbort exception" << std::endl;
      std::cout << e.what() << std::endl;
    }
    m_encoding_time = commit_abortkey->encoding_time();
    m_cross_rack_time = commit_abortkey->cross_rack_time();
    return grpc::Status::OK;
  }

  grpc::Status
  CoordinatorImpl::checkCommitAbort(grpc::ServerContext *context,
                                    const coordinator_proto::AskIfSuccess *key_opp,
                                    coordinator_proto::RepIfSuccess *reply)
  {
    std::unique_lock<std::mutex> lck(m_mutex);
    std::string key = key_opp->key();
    ECProject::OpperateType opp = (ECProject::OpperateType)key_opp->opp();
    int stripe_id = key_opp->stripe_id();
    if (opp == SET)
    {
      while (m_object_commit_table.find(key) == m_object_commit_table.end())
      {
        cv.wait(lck);
      }
    }
    else if (opp == DEL)
    {
      if (stripe_id < 0)
      {
        while (m_object_commit_table.find(key) != m_object_commit_table.end())
        {
          cv.wait(lck);
        }
      }
      else
      {
        auto it = std::find(m_stripe_deleting_table.begin(), m_stripe_deleting_table.end(), stripe_id);
        while (it != m_stripe_deleting_table.end())
        {
          cv.wait(lck);
          it = std::find(m_stripe_deleting_table.begin(), m_stripe_deleting_table.end(), stripe_id);
        }
      }
    }
    reply->set_ifcommit(true);
    reply->set_set_time(m_set_time);
    reply->set_encoding_time(m_encoding_time);
    reply->set_cross_rack_time(m_cross_rack_time);
    m_set_time = 0;
    m_encoding_time = 0;
    m_cross_rack_time = 0;
    return grpc::Status::OK;
  }

  // merge
  grpc::Status CoordinatorImpl::requestMerge(
      grpc::ServerContext *context,
      const coordinator_proto::NumberOfStripesToMerge *numofstripe,
      coordinator_proto::RepIfMerged *mergeReplyClient)
  {
    int l = m_encode_parameters.l_localparityblock;
    int b = m_encode_parameters.b_datapergroup;
    int g_m = m_encode_parameters.g_m_globalparityblock;
    int m = b % (g_m + 1);
    EncodeType encodetype = m_encode_parameters.encodetype;
    int num_of_stripes = m_encode_parameters.x_stripepermergegroup;
    int total_stripes = m_stripe_table.size();
    if (total_stripes % num_of_stripes != 0)
    {
      mergeReplyClient->set_ifmerged(false);
      return grpc::Status::OK;
    }
    MultiStripesPlacementType m_s_placementtype = m_encode_parameters.m_stripe_placementtype;
    if (m_s_placementtype == DIS || m_s_placementtype == OPT)
    {
      int num_stripepergroup = m_merge_groups[0].size();
      if (num_stripepergroup % num_of_stripes != 0)
      {
        std::cout << "Not enough stripes in each merging group!" << std::endl;
        mergeReplyClient->set_ifmerged(false);
        return grpc::Status::OK;
      }
    }
    if (encodetype == HPC)
    {
      request_merge_hpc(mergeReplyClient);
    }
    else if (encodetype == Azure_LRC)
    {
      request_merge_lrc(l, b, g_m, m, num_of_stripes, mergeReplyClient);
    }
    else
    {
      request_merge_rs(mergeReplyClient);
    }
    // mlec

    return grpc::Status::OK;
  }

  grpc::Status CoordinatorImpl::requestRepair(
      grpc::ServerContext *context,
      const coordinator_proto::FailureInfo *failures,
      coordinator_proto::RepIfRepaired *repairReplyClient)
  {
    bool isblock = failures->isblock();
    int stripe_id = -1;
    auto blocks_or_nodes = std::make_shared<std::vector<int>>();
    if (isblock)
    {
      stripe_id = failures->stripe_id();
    }
    for (int i = 0; i < int(failures->blocks_or_nodes_size()); i++)
    {
      blocks_or_nodes->push_back(failures->blocks_or_nodes(i));
    }

    request_repair(isblock, stripe_id, blocks_or_nodes, repairReplyClient);

    return grpc::Status::OK;
  }

} // namespace ECProject
