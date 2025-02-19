#include "client.h"
#include "coordinator.grpc.pb.h"

#include <asio.hpp>
namespace ECProject
{
  std::string Client::sayHelloToCoordinatorByGrpc(std::string hello)
  {
    coordinator_proto::RequestToCoordinator request;
    request.set_name(hello);
    coordinator_proto::ReplyFromCoordinator reply;
    grpc::ClientContext context;
    grpc::Status status = m_coordinator_ptr->sayHelloToCoordinator(&context, request, &reply);
    if (status.ok())
    {
      return reply.message();
    }
    else
    {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }
  // grpc, set the parameters stored in the variable of m_encode_parameters in coordinator
  bool Client::SetParameterByGrpc(ECSchema input_ecschema)
  {
    int k = input_ecschema.k_datablock;
    int l = input_ecschema.l_localparityblock;
    int g_m = input_ecschema.g_m_globalparityblock;
    int b = input_ecschema.b_datapergroup;
    int k1 = input_ecschema.k1_col_datablock;
    int m1 = input_ecschema.m1_col_parityblock;
    int k2 = input_ecschema.k2_row_datablock;
    int m2 = input_ecschema.m2_row_parityblock;
    EncodeType encodetype = input_ecschema.encodetype;
    if (encodetype == ECProject::Azure_LRC)
    {
      int m = b % (g_m + 1);
      if (b != k / l || (m != 0 && encodetype == Azure_LRC && g_m % m != 0))
      {
        std::cout << "Set parameters failed! Illegal parameters!" << std::endl;
        exit(0);
      }
    }

    coordinator_proto::Parameter parameter;
    parameter.set_partial_decoding((int)input_ecschema.partial_decoding);
    parameter.set_approach((int)input_ecschema.approach);
    parameter.set_encodetype(encodetype);
    parameter.set_s_stripe_placementtype(input_ecschema.s_stripe_placementtype);
    parameter.set_m_stripe_placementtype(input_ecschema.m_stripe_placementtype);
    parameter.set_k_datablock(k);
    parameter.set_l_localparityblock(l);
    parameter.set_g_m_globalparityblock(g_m);
    parameter.set_b_datapergroup(b);
    parameter.set_k1_col_datablock(k1);
    parameter.set_m1_col_parityblock(m1);
    parameter.set_k2_row_datablock(k2);
    parameter.set_m2_row_parityblock(m2);
    parameter.set_x_stripepermergegroup(input_ecschema.x_stripepermergegroup);
    grpc::ClientContext context;
    coordinator_proto::RepIfSetParaSuccess reply;
    grpc::Status status = m_coordinator_ptr->setParameter(&context, parameter, &reply);
    if (status.ok())
    {
      return reply.ifsetparameter();
    }
    else
    {
      std::cout << status.error_code() << ": " << status.error_message() << std::endl;
      return false;
    }
  }
  /*
    Function: set
    1. send the set request including the information of key and valuesize to the coordinator
    2. get the address of proxy
    3. send the value to the proxy by socket
  */
  bool Client::set(std::string key, std::string value)
  {
    struct timeval start_time, end_time;
    gettimeofday(&start_time, NULL);
    grpc::ClientContext get_proxy_ip_port;
    coordinator_proto::RequestProxyIPPort request;
    coordinator_proto::ReplyProxyIPPort reply;
    request.set_key(key);
    request.set_valuesizebytes(value.size());
    grpc::Status status = m_coordinator_ptr->uploadOriginKeyValue(&get_proxy_ip_port, request, &reply);
    if (!status.ok())
    {
      std::cout << "[SET] upload data failed!" << std::endl;
      return false;
    }
    else
    {
      std::string proxy_ip = reply.proxyip();
      int proxy_port = reply.proxyport();
      std::cout << "[SET] Send " << key << " to proxy_address:" << proxy_ip << ":" << proxy_port << std::endl;
      // read to send the value
      asio::io_context io_context;
      asio::error_code error;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::resolver::results_type endpoints =
          resolver.resolve(proxy_ip, std::to_string(proxy_port));
      asio::ip::tcp::socket sock_data(io_context);
      asio::connect(sock_data, endpoints);

      // std::cout << "[SET] key_size:" << key.size() << ", value_size:" << value.size();
      // std::cout << ", proxy_address:" << proxy_ip << ":" << proxy_port << std::endl;
      asio::write(sock_data, asio::buffer(key, key.size()), error);
      asio::write(sock_data, asio::buffer(value, value.size()), error);
      asio::error_code ignore_ec;
      sock_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
      sock_data.close(ignore_ec);

      // check if metadata is saved successfully
      grpc::ClientContext check_commit;
      coordinator_proto::AskIfSuccess request;
      request.set_key(key);
      OpperateType opp = SET;
      request.set_opp(opp);
      coordinator_proto::RepIfSuccess reply;
      grpc::Status status;
      status = m_coordinator_ptr->checkCommitAbort(&check_commit, request, &reply);
      if (status.ok())
      {
        if (reply.ifcommit())
        {
          gettimeofday(&end_time, NULL);
          double set_time = end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
          std::cout << "[SET_TIME] total: " << set_time
                    << "s, encoding: " << reply.encoding_time()
                    << "s, cross-rack: " << reply.cross_rack_time() << "s.\n";
          return true;
        }
        else
        {
          std::cout << "[SET] " << key << " not commit!!!!!" << std::endl;
        }
      }
      else
      {
        std::cout << "[SET] " << key << " Fail to check!!!!!" << std::endl;
      }
    }
    return false;
  }
  /*
    Function: get
    1. send the get request including the information of key and clientipport to the coordinator
    2. accept the value transferred from the proxy
  */
  bool Client::get(std::string key, std::string &value)
  {
    grpc::ClientContext context;
    coordinator_proto::KeyAndClientIP request;
    request.set_key(key);
    request.set_clientip(m_clientIPForGet);
    request.set_clientport(m_clientPortForGet);
    // request
    coordinator_proto::RepIfGetSuccess reply;
    grpc::Status status = m_coordinator_ptr->getValue(&context, request, &reply);
    asio::ip::tcp::socket socket_data(io_context);
    int value_size = reply.valuesizebytes();
    acceptor.accept(socket_data);
    asio::error_code error;
    std::vector<char> buf_key(key.size());
    std::vector<char> buf(value_size);
    // read from socket
    size_t len = asio::read(socket_data, asio::buffer(buf_key, key.size()), error);
    int flag = 1;
    for (int i = 0; i < int(key.size()); i++)
    {
      if (key[i] != buf_key[i])
      {
        flag = 0;
      }
    }
    if (flag)
    {
      len = asio::read(socket_data, asio::buffer(buf, value_size), error);
    }
    else
    {
      std::cout << "[GET] key not matches!" << std::endl;
    }
    asio::error_code ignore_ec;
    socket_data.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
    socket_data.close(ignore_ec);
    if (flag)
    {
      std::cout << "[GET] get key: " << key << " ,valuesize: " << len << std::endl;
    }
    value = std::string(buf.data(), buf.size());
    return true;
  }

  /*
    Function: delete
    1. send the get request including the information of key to the coordinator
  */
  bool Client::delete_key(std::string key)
  {
    grpc::ClientContext context;
    coordinator_proto::KeyFromClient request;
    request.set_key(key);
    coordinator_proto::RepIfDeling reply;
    grpc::Status status = m_coordinator_ptr->delByKey(&context, request, &reply);
    if (status.ok())
    {
      if (reply.ifdeling())
      {
        std::cout << "[DEL] deleting " << key << std::endl;
      }
      else
      {
        std::cout << "[DEL] delete failed!" << std::endl;
      }
    }
    // check if metadata is saved successfully
    grpc::ClientContext check_commit;
    coordinator_proto::AskIfSuccess req;
    req.set_key(key);
    ECProject::OpperateType opp = DEL;
    req.set_opp(opp);
    req.set_stripe_id(-1);
    coordinator_proto::RepIfSuccess rep;
    grpc::Status stat;
    stat = m_coordinator_ptr->checkCommitAbort(&check_commit, req, &rep);
    if (stat.ok())
    {
      if (rep.ifcommit())
      {
        return true;
      }
      else
      {
        std::cout << "[DEL]" << key << " not delete!!!!!";
      }
    }
    else
    {
      std::cout << "[DEL]" << key << " Fail to check!!!!!";
    }
    return false;
  }

  bool Client::delete_stripe(int stripe_id)
  {
    grpc::ClientContext context;
    coordinator_proto::StripeIdFromClient request;
    request.set_stripe_id(stripe_id);
    coordinator_proto::RepIfDeling reply;
    grpc::Status status = m_coordinator_ptr->delByStripe(&context, request, &reply);
    if (status.ok())
    {
      if (reply.ifdeling())
      {
        std::cout << "[DEL] deleting Stripe " << stripe_id << std::endl;
      }
      else
      {
        std::cout << "[DEL] delete failed!" << std::endl;
      }
    }
    // check if metadata is saved successfully
    grpc::ClientContext check_commit;
    coordinator_proto::AskIfSuccess req;
    req.set_key("");
    ECProject::OpperateType opp = DEL;
    req.set_opp(opp);
    req.set_stripe_id(stripe_id);
    coordinator_proto::RepIfSuccess rep;
    grpc::Status stat;
    stat = m_coordinator_ptr->checkCommitAbort(&check_commit, req, &rep);
    if (stat.ok())
    {
      if (rep.ifcommit())
      {
        return true;
      }
      else
      {
        std::cout << "[DEL] Stripe" << stripe_id << " not delete!!!!!";
      }
    }
    else
    {
      std::cout << "[DEL] Stripe" << stripe_id << " Fail to check!!!!!";
    }
    return false;
  }

  bool Client::delete_all_stripes()
  {
    grpc::ClientContext context;
    coordinator_proto::RepStripeIds rep;
    coordinator_proto::RequestToCoordinator req;
    grpc::Status status = m_coordinator_ptr->listStripes(&context, req, &rep);
    if (status.ok())
    {
      std::cout << "Deleting all stripes!" << std::endl;
      for (int i = 0; i < int(rep.stripe_ids_size()); i++)
      {
        delete_stripe(rep.stripe_ids(i));
      }
      return true;
    }
    return false;
  }

  /*
    Function: merge
    1. send the merge request including the information of num_of_stripes_tomerge to the coordinator
  */
  double Client::merge(int num_of_stripes)
  {
    grpc::ClientContext context;
    coordinator_proto::NumberOfStripesToMerge request;
    request.set_num_of_stripes(num_of_stripes);
    coordinator_proto::RepIfMerged reply;
    grpc::Status status = m_coordinator_ptr->requestMerge(&context, request, &reply);
    double cost = 0;
    if (status.ok())
    {
      if (reply.ifmerged())
      {
        std::cout << "<Simulation> Cross Rack: " << reply.cross_rack_num() << " blocks." << std::endl;
        std::cout << "|-- Global parity block recalulation: " << reply.gc() << std::endl;
        std::cout << "|-- Data block relocation: " << reply.dc() << std::endl;
        std::cout << "|-- meta: " << reply.meta_time() << std::endl;
        std::cout << "|-- encoding: " << reply.encoding_time() << std::endl;
        std::cout << "|-- cross-rack: " << reply.cross_rack_time() << std::endl;
        std::cout << "|-- inner-rack: " << reply.inner_rack_time() << std::endl;
        cost = reply.gc() + reply.dc();
        // std::cout << "Stage cost: " << cost << std::endl;
      }
      else
      {
        std::cout << "[MERGE] merge failed!" << std::endl;
      }
    }
    return cost;
  }

  /*
    Function: node repair
    1. send the repair request including the information of failed_node_list to the coordinator
  */
  int Client::node_repair(std::vector<int> &failed_node_list, double &cost, double &de_time, int &cross_rack_num, double &meta_time, double &cross_rt, double &inner_rt)
  {
    grpc::ClientContext context;
    coordinator_proto::FailureInfo request;
    request.set_isblock(false);
    request.set_stripe_id(-1);
    for (int i = 0; i < int(failed_node_list.size()); i++)
    {
      request.add_blocks_or_nodes(failed_node_list[i]);
    }
    coordinator_proto::RepIfRepaired reply;
    grpc::Status status = m_coordinator_ptr->requestRepair(&context, request, &reply);
    int failed_stripe_num = 0;
    if (status.ok())
    {
      if (reply.ifrepaired())
      {
        cost = reply.rc();
        cross_rack_num = reply.cross_rack_num();
        failed_stripe_num = reply.failed_stripe_num();
        de_time = reply.decoding_time();
        meta_time = reply.meta_time();
        cross_rt = reply.cross_rack_time();
        inner_rt = reply.inner_rack_time();
      }
      else
      {
        std::cout << "[Repair] repair failed!" << std::endl;
      }
    }
    return failed_stripe_num;
  }
  int Client::block_repair(std::vector<int> &failed_block_list, int stripe_id, double &cost, double &de_time, int &cross_rack_num, double &meta_time, double &cross_rt, double &inner_rt)
  {
    grpc::ClientContext context;
    coordinator_proto::FailureInfo request;
    request.set_isblock(true);
    request.set_stripe_id(stripe_id);
    for (int i = 0; i < int(failed_block_list.size()); i++)
    {
      request.add_blocks_or_nodes(failed_block_list[i]);
    }
    coordinator_proto::RepIfRepaired reply;
    grpc::Status status = m_coordinator_ptr->requestRepair(&context, request, &reply);
    int failed_stripe_num = 0;
    if (status.ok())
    {
      if (reply.ifrepaired())
      {
        cost = reply.rc();
        cross_rack_num = reply.cross_rack_num();
        failed_stripe_num = reply.failed_stripe_num();
        de_time = reply.decoding_time();
        meta_time = reply.meta_time();
        cross_rt = reply.cross_rack_time();
        inner_rt = reply.inner_rack_time();
      }
      else
      {
        std::cout << "[Repair] repair failed!" << std::endl;
      }
    }
    return failed_stripe_num;
  }
} // namespace ECProject