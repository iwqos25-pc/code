#include "proxy.h"
#include "jerasure.h"
#include "reed_sol.h"
#include "tinyxml2.h"
#include "toolbox.h"
#include "lrc.h"
#include "pc.h"
#include <thread>
#include <cassert>
#include <string>
#include <fstream>
template <typename T>
inline T ceil(T const &A, T const &B)
{
  return T((A + B - 1) / B);
};

inline std::string generate_string(int length)
{
  static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  std::string random_string;
  random_string.reserve(length);

  for (int i = 0; i < length; i++)
  {
    random_string += alphanum[i % 62];
  }

  return random_string;
}
namespace ECProject
{
  bool ProxyImpl::init_coordinator()
  {
    m_coordinator_ptr = coordinator_proto::coordinatorService::NewStub(grpc::CreateChannel(m_coordinator_address, grpc::InsecureChannelCredentials()));
    // coordinator_proto::RequestToCoordinator req;
    // coordinator_proto::ReplyFromCoordinator rep;
    // grpc::ClientContext context;
    // std::string proxy_info = "Proxy [" + proxy_ip_port + "]";
    // req.set_name(proxy_info);
    // grpc::Status status;
    // status = m_coordinator_ptr->checkalive(&context, req, &rep);
    // if (status.ok())
    // {
    //   std::cout << "[Coordinator Check] ok from " << m_coordinator_address << std::endl;
    // }
    // else
    // {
    //   std::cout << "[Coordinator Check] failed to connect " << m_coordinator_address << std::endl;
    // }
    return true;
  }

  bool ProxyImpl::init_datanodes(std::string m_datanodeinfo_path)
  {
    tinyxml2::XMLDocument xml;
    xml.LoadFile(m_datanodeinfo_path.c_str());
    tinyxml2::XMLElement *root = xml.RootElement();
    for (tinyxml2::XMLElement *rack = root->FirstChildElement(); rack != nullptr; rack = rack->NextSiblingElement())
    {
      std::string rack_id(rack->Attribute("id"));
      std::string proxy(rack->Attribute("proxy"));
      if (proxy == proxy_ip_port)
      {
        m_self_rack_id = std::stoi(rack_id);
      }
      for (tinyxml2::XMLElement *node = rack->FirstChildElement()->FirstChildElement(); node != nullptr; node = node->NextSiblingElement())
      {
        std::string node_uri(node->Attribute("uri"));
        auto _stub = datanode_proto::datanodeService::NewStub(grpc::CreateChannel(node_uri, grpc::InsecureChannelCredentials()));
        // datanode_proto::CheckaliveCMD cmd;
        // datanode_proto::RequestResult result;
        // grpc::ClientContext context;
        // std::string proxy_info = "Proxy [" + proxy_ip_port + "]";
        // cmd.set_name(proxy_info);
        // grpc::Status status;
        // status = _stub->checkalive(&context, cmd, &result);
        // if (status.ok())
        // {
        //   // std::cout << "[Datanode Check] ok from " << node_uri << std::endl;
        // }
        // else
        // {
        //   std::cout << "[Datanode Check] failed to connect " << node_uri << std::endl;
        // }
        m_datanode_ptrs.insert(std::make_pair(node_uri, std::move(_stub)));
        m_datanode2rack.insert(std::make_pair(node_uri, std::stoi(rack_id)));
      }
    }
    // init networkcore
    for (int i = 0; i < int(m_networkcore.size()); i++)
    {
      auto _stub = datanode_proto::datanodeService::NewStub(grpc::CreateChannel(m_networkcore[i], grpc::InsecureChannelCredentials()));
      m_datanode_ptrs.insert(std::make_pair(m_networkcore[i], std::move(_stub)));
    }

    return true;
  }

  grpc::Status ProxyImpl::checkalive(grpc::ServerContext *context,
                                     const proxy_proto::CheckaliveCMD *request,
                                     proxy_proto::RequestResult *response)
  {

    std::cout << "[Proxy] checkalive" << request->name() << std::endl;
    response->set_message(false);
    init_coordinator();
    return grpc::Status::OK;
  }

  bool ProxyImpl::SetToDatanode(const char *key, size_t key_length, const char *value, size_t value_length, const char *ip, int port, int offset)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::SetInfo set_info;
      datanode_proto::RequestResult result;
      set_info.set_block_key(std::string(key));
      set_info.set_block_size(value_length);
      set_info.set_proxy_ip(m_ip);
      set_info.set_proxy_port(m_port + offset);
      set_info.set_ispull(false);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleSet(&context, set_info, &result);

      asio::error_code error;
      asio::io_context io_context;
      asio::ip::tcp::socket socket(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      asio::error_code con_error;
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + 20)}), con_error);
      if (!con_error && IF_DEBUG)
      {
        std::cout << "Connect to " << ip << ":" << port + 20 << " success!" << std::endl;
      }

      asio::write(socket, asio::buffer(value, value_length), error);

      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_rack_id << "][SET]"
                  << "Write " << key << " to socket finish! With length of " << strlen(value) << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::GetFromDatanode(const char *key, size_t key_length, char *value, size_t value_length, const char *ip, int port, int offset)
  {
    try
    {
      // ready to recieve
      char *buf = new char[value_length];
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_rack_id << "][GET]"
                  << " Ready to recieve data from datanode " << std::endl;
      }

      grpc::ClientContext context;
      datanode_proto::GetInfo get_info;
      datanode_proto::RequestResult result;
      get_info.set_block_key(std::string(key));
      get_info.set_block_size(value_length);
      get_info.set_proxy_ip(m_ip);
      get_info.set_proxy_port(m_port + offset);
      std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
      grpc::Status stat = m_datanode_ptrs[node_ip_port]->handleGet(&context, get_info, &result);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_rack_id << "][GET]"
                  << " Call datanode to handle get " << key << std::endl;
      }

      asio::io_context io_context;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::socket socket(io_context);
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + 20)}));
      asio::error_code ec;

      asio::read(socket, asio::buffer(buf, value_length), ec);

      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_rack_id << "][GET]"
                  << " Read data from socket with length of " << value_length << std::endl;
      }
      memcpy(value, buf, value_length);
      delete buf;
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::DelInDatanode(std::string key, std::string node_ip_port)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::DelInfo delinfo;
      datanode_proto::RequestResult response;
      delinfo.set_block_key(key);
      grpc::Status status = m_datanode_ptrs[node_ip_port]->handleDelete(&context, delinfo, &response);
      if (status.ok() && IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_rack_id << "][DEL] delete block " << key << " success!" << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::TransferToNetworkCore(const char *key, const char *value, size_t value_length, bool ifset, int idx)
  {
    try
    {
      grpc::ClientContext context;
      datanode_proto::TransferInfo trans_info;
      datanode_proto::RequestResult result;
      trans_info.set_value_key(std::string(key));
      trans_info.set_value_size(value_length);
      trans_info.set_ifset(ifset);
      grpc::Status stat = m_datanode_ptrs[m_networkcore[idx]]->handleTransfer(&context, trans_info, &result);

      std::string ip;
      int port;
      std::stringstream ss(m_networkcore[idx]);
      std::getline(ss, ip, ':');
      ss >> port;

      std::string type = "cross-rack";

      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_rack_id << "][" << type << " Transfer] " << m_networkcore[idx] << " address " << ip << ":" << port << std::endl;
      }

      asio::error_code error;
      asio::io_context io_context;
      asio::ip::tcp::socket socket(io_context);
      asio::ip::tcp::resolver resolver(io_context);
      asio::error_code con_error;
      asio::connect(socket, resolver.resolve({std::string(ip), std::to_string(port + 20)}), con_error);
      if (!con_error && IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_rack_id << "][ " << type << " Transfer]" << "Connect to " << ip << ":" << port + 20 << " success!" << std::endl;
      }

      asio::write(socket, asio::buffer(value, value_length), error);

      asio::error_code ignore_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
      socket.close(ignore_ec);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_rack_id << "][ " << type << " Transfer]"
                  << "Write " << key << " to socket finish! With length of " << strlen(value) << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return true;
  }

  bool ProxyImpl::BlockRelocation(const char *key, size_t value_length, const char *src_ip, int src_port, const char *des_ip, int des_port)
  {
    try
    {
      gettimeofday(&start_time, NULL);
      grpc::ClientContext context;
      datanode_proto::GetInfo get_info;
      datanode_proto::RequestResult result;
      get_info.set_block_key(std::string(key));
      get_info.set_block_size(value_length);
      get_info.set_proxy_ip(m_ip);
      get_info.set_proxy_port(m_port);
      std::string s_node_ip_port = std::string(src_ip) + ":" + std::to_string(src_port);
      grpc::Status stat = m_datanode_ptrs[s_node_ip_port]->handleGet(&context, get_info, &result);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_rack_id << "][Relocation]"
                  << " Call datanode" << src_port << " to handle get " << key << std::endl;
      }
      gettimeofday(&end_time, NULL);
      inner_rack_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

      grpc::ClientContext s_context;
      datanode_proto::SetInfo set_info;
      datanode_proto::RequestResult s_result;
      set_info.set_block_key(std::string(key));
      set_info.set_block_size(value_length);
      set_info.set_proxy_ip(src_ip);
      set_info.set_proxy_port(src_port + 20);
      set_info.set_ispull(true);
      std::string d_node_ip_port = std::string(des_ip) + ":" + std::to_string(des_port);
      grpc::Status s_stat = m_datanode_ptrs[d_node_ip_port]->handleSet(&s_context, set_info, &s_result);
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_rack_id << "][Relocation]"
                  << " Call datanode" << des_port << " to handle set " << key << std::endl;
      }
      if (s_stat.ok() && IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_rack_id << "][Relocation] relocate block " << key << " success!" << std::endl;
      }

      gettimeofday(&start_time, NULL);
      // simulate cross-rack transfer
      int idx = 0;
      int s_rack = m_datanode2rack[s_node_ip_port];
      int d_rack = m_datanode2rack[d_node_ip_port];
      if (s_rack != d_rack)
      {
        std::string temp_value = generate_string(value_length);
        TransferToNetworkCore(key, temp_value.c_str(), value_length, false, idx);
      }
      gettimeofday(&end_time, NULL);
      cross_rack_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }
    return true;
  }

  grpc::Status ProxyImpl::encodeAndSetObject(
      grpc::ServerContext *context,
      const proxy_proto::ObjectAndPlacement *object_and_placement,
      proxy_proto::SetReply *response)
  {
    std::string key = object_and_placement->key();
    int value_size_bytes = object_and_placement->valuesizebyte();
    int k = object_and_placement->k();
    int g_m = object_and_placement->g_m();
    int l = object_and_placement->l();
    int k1 = object_and_placement->k1();
    int m1 = object_and_placement->m1();
    int k2 = object_and_placement->k2();
    int m2 = object_and_placement->m2();
    int x = object_and_placement->x();
    int seri_num = object_and_placement->seri_num();
    bool isvertical = object_and_placement->isvertical();
    int block_size = object_and_placement->block_size();
    ECProject::EncodeType encode_type = (ECProject::EncodeType)object_and_placement->encode_type();
    std::vector<std::pair<std::string, std::pair<std::string, int>>> keys_nodes;
    for (int i = 0; i < object_and_placement->datanodeip_size(); i++)
    {
      keys_nodes.push_back(std::make_pair(object_and_placement->blockkeys(i), std::make_pair(object_and_placement->datanodeip(i), object_and_placement->datanodeport(i))));
    }
    auto encode_and_save = [this, key, value_size_bytes, k, g_m, l, k1, k2, m1, m2, x, seri_num, block_size, isvertical, keys_nodes, encode_type, response]() mutable
    {
      try
      {
        // read the key and value in the socket sent by client
        asio::ip::tcp::socket socket_data(io_context);
        acceptor.accept(socket_data);
        asio::error_code error;

        int extend_value_size_byte = block_size * k;
        std::vector<char> buf_key(key.size());
        std::vector<char> v_buf(extend_value_size_byte);
        for (int i = value_size_bytes; i < extend_value_size_byte; i++)
        {
          v_buf[i] = '0';
        }

        asio::read(socket_data, asio::buffer(buf_key, key.size()), error);
        if (error == asio::error::eof)
        {
          std::cout << "error == asio::error::eof" << std::endl;
        }
        else if (error)
        {
          throw asio::system_error(error);
        }
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_rack_id << "][SET]"
                    << "Check key " << buf_key.data() << std::endl;
        }
        // check the key
        bool flag = true;
        for (int i = 0; i < int(key.size()); i++)
        {
          if (key[i] != buf_key[i])
          {
            flag = false;
          }
        }
        if (flag)
        {
          if (IF_DEBUG)
          {
            std::cout << "[Proxy" << m_self_rack_id << "][SET]"
                      << "Read value of " << buf_key.data() << std::endl;
          }
          asio::read(socket_data, asio::buffer(v_buf.data(), value_size_bytes), error);
        }
        asio::error_code ignore_ec;
        socket_data.shutdown(asio::ip::tcp::socket::shutdown_receive, ignore_ec);
        socket_data.close(ignore_ec);

        // set the blocks to the datanode
        char *buf = v_buf.data();
        auto send_to_datanode = [this](int j, int d_num, std::string block_key, char **data, char **coding, int block_size, std::pair<std::string, int> ip_and_port)
        {
          if (IF_DEBUG)
          {
            std::cout << "[Proxy" << m_self_rack_id << "][SET]"
                      << "Thread " << j << " send " << block_key << " to Datanode" << ip_and_port.second << std::endl;
          }
          if (j < d_num)
          {
            SetToDatanode(block_key.c_str(), block_key.size(), data[j], block_size, ip_and_port.first.c_str(), ip_and_port.second, j + 2);
          }
          else
          {
            SetToDatanode(block_key.c_str(), block_key.size(), coding[j - d_num], block_size, ip_and_port.first.c_str(), ip_and_port.second, j + 2);
          }
        };

        int data_num = k;
        int coding_num = g_m;
        if (encode_type == Azure_LRC)
        {
          coding_num = g_m + l;
        }
        else if (encode_type == HPC || encode_type == MLEC)
        {
          data_num = k1 * k2;
          coding_num = k2 * m1 + (k1 + m1) * m2;
        }
        // calculate parity blocks
        std::vector<char *> v_data(data_num);
        std::vector<char *> v_coding(coding_num);
        char **data = (char **)v_data.data();
        char **coding = (char **)v_coding.data();

        std::vector<std::vector<char>> v_coding_area(coding_num, std::vector<char>(block_size));
        for (int j = 0; j < data_num; j++)
        {
          data[j] = &buf[j * block_size];
        }
        for (int j = 0; j < coding_num; j++)
        {
          coding[j] = v_coding_area[j].data();
        }
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_rack_id << "][SET]"
                    << "Encode value with size of " << v_buf.size() << std::endl;
        }
        int send_num;
        double t_time = 0.0;
        std::string et = "";
        gettimeofday(&start_time, NULL);
        if (encode_type == RS)
        {
          encode_RS(k, g_m, data, coding, block_size);
          send_num = k + g_m;
          et = "(RS)";
        }
        else if (encode_type == Azure_LRC)
        {
          encode_LRC(k, g_m, l, data, coding, block_size, encode_type);
          send_num = k + g_m + l;
          et = "(Azure_LRC)";
        }
        else if (encode_type == MLEC)
        {
          encode_PC(k1, m1, k2, m2, data, coding, block_size);
          send_num = (k1 + m1) * (k2 + m2);
          et = "(MLEC)";
        }
        else if (encode_type == HPC)
        {
          encode_HPC(x, k1, m1, k2, m2, data, coding, block_size, isvertical, seri_num);
          send_num = (k1 + m1) * (k2 + m2);
          et = "(HPC)";
        }
        gettimeofday(&end_time, NULL);
        t_time = end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
        encoding_time = t_time;
        std::cout << et << "Encode Time: " << t_time << " (block_size=" << block_size / (1024 * 1024) << "MB)." << std::endl;
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_rack_id << "][SET]"
                    << "Distribute blocks to datanodes" << std::endl;
        }
        if (send_num != int(keys_nodes.size()))
        {
          std::cout << "[Proxy" << m_self_rack_id << "][SET] Error! Blocks number not matches!" << std::endl;
        }
        std::vector<std::thread> senders;
        for (int j = 0; j < send_num; j++)
        {
          std::string block_key = keys_nodes[j].first;
          std::pair<std::string, int> &ip_and_port = keys_nodes[j].second;
          senders.push_back(std::thread(send_to_datanode, j, data_num, block_key, data, coding, block_size, ip_and_port));
        }
        for (int j = 0; j < int(senders.size()); j++)
        {
          senders[j].join();
        }

        gettimeofday(&start_time, NULL);
        if (IF_TEST_THROUGHPUT)
        {
          // simulate cross-rack transfer
          int cross_rack_num = 0;
          int idx = 0;
          for (int j = 0; j < send_num; j++)
          {
            std::pair<std::string, int> &ip_and_port = keys_nodes[j].second;
            std::string ip_port = ip_and_port.first + ":" + std::to_string(ip_and_port.second);
            int t_rack_id = m_datanode2rack[ip_port];
            if (t_rack_id != m_self_rack_id)
            {
              cross_rack_num++;
            }
          }
          if (cross_rack_num > 0)
          {
            std::string temp_key = "temp";
            int val_len = block_size * cross_rack_num;
            std::string temp_value = generate_string(val_len);
            TransferToNetworkCore(temp_key.c_str(), temp_value.c_str(), val_len, false, idx);
          }
        }
        gettimeofday(&end_time, NULL);
        t_time = end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
        cross_rack_time = t_time;

        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_rack_id << "][SET]"
                    << "Finish distributing blocks!" << std::endl;
        }
        coordinator_proto::CommitAbortKey commit_abort_key;
        coordinator_proto::ReplyFromCoordinator result;
        grpc::ClientContext context;
        ECProject::OpperateType opp = SET;
        commit_abort_key.set_opp(opp);
        commit_abort_key.set_key(key);
        commit_abort_key.set_ifcommitmetadata(true);
        commit_abort_key.set_encoding_time(encoding_time);
        commit_abort_key.set_cross_rack_time(cross_rack_time);
        encoding_time = 0;
        cross_rack_time = 0;
        grpc::Status status;
        status = m_coordinator_ptr->reportCommitAbort(&context, commit_abort_key, &result);
        if (status.ok() && IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_rack_id << "][SET]"
                    << "[SET] report to coordinator success" << std::endl;
        }
        else if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_rack_id << "][SET]"
                    << " report to coordinator fail!" << std::endl;
        }
      }
      catch (std::exception &e)
      {
        std::cout << "exception in encode_and_save" << std::endl;
        std::cout << e.what() << std::endl;
      }
    };
    try
    {
      if (IF_DEBUG)
      {
        std::cout << "[Proxy][SET] Handle encode and set" << std::endl;
      }
      std::thread my_thread(encode_and_save);
      my_thread.detach();
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::decodeAndGetObject(
      grpc::ServerContext *context,
      const proxy_proto::ObjectAndPlacement *object_and_placement,
      proxy_proto::GetReply *response)
  {
    ECProject::EncodeType encode_type = (ECProject::EncodeType)object_and_placement->encode_type();
    std::string key = object_and_placement->key();
    int k = object_and_placement->k();
    int g_m = object_and_placement->g_m();
    int l = object_and_placement->l();
    int k1 = object_and_placement->k1();
    int m1 = object_and_placement->m1();
    int k2 = object_and_placement->k2();
    int m2 = object_and_placement->m2();
    int x = object_and_placement->x();
    int seri_num = object_and_placement->seri_num();
    int value_size_bytes = object_and_placement->valuesizebyte();
    int block_size = ceil(value_size_bytes, k);
    std::string clientip = object_and_placement->clientip();
    int clientport = object_and_placement->clientport();
    int stripe_id = object_and_placement->stripe_id();

    if (encode_type == ECProject::HPC || encode_type == ECProject::MLEC)
    {
      k = k1 * k2;
    }

    std::vector<std::pair<std::string, std::pair<std::string, int>>> keys_nodes;
    std::vector<int> block_idxs;
    int block_num = object_and_placement->datanodeip_size();
    for (int i = 0; i < block_num; i++)
    {
      block_idxs.push_back(object_and_placement->blockids(i));
      keys_nodes.push_back(std::make_pair(object_and_placement->blockkeys(i), std::make_pair(object_and_placement->datanodeip(i), object_and_placement->datanodeport(i))));
    }

    auto decode_and_get = [this, key, k, g_m, l, k1, m1, k2, m2, x, seri_num, block_num, block_size, value_size_bytes, stripe_id,
                           clientip, clientport, keys_nodes, block_idxs, encode_type]() mutable
    {
      auto blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
      auto blocks_idx_ptr = std::make_shared<std::vector<int>>();
      auto myLock_ptr = std::make_shared<std::mutex>();
      auto cv_ptr = std::make_shared<std::condition_variable>();

      std::vector<char *> v_data(k);
      char **data = v_data.data();

      auto getFromNode = [this, k, blocks_ptr, blocks_idx_ptr, myLock_ptr, cv_ptr](int block_idx, std::string block_key, int block_size, std::string ip, int port)
      {
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_rack_id << "][GET]"
                    << "Block " << block_idx << " with key " << block_key << " from Datanode" << ip << ":" << port << std::endl;
        }

        std::vector<char> temp(block_size);
        bool ret = GetFromDatanode(block_key.c_str(), block_key.size(), temp.data(), block_size, ip.c_str(), port, block_idx + 2);

        if (!ret)
        {
          std::cout << "getFromNode !ret" << std::endl;
          return;
        }
        myLock_ptr->lock();
        blocks_ptr->push_back(temp);
        blocks_idx_ptr->push_back(block_idx);
        myLock_ptr->unlock();
      };

      std::vector<std::vector<char>> v_data_area(k, std::vector<char>(block_size));
      for (int j = 0; j < k; j++)
      {
        data[j] = v_data_area[j].data();
      }
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_rack_id << "][GET]"
                  << "ready to get blocks from datanodes!" << std::endl;
      }
      std::vector<std::thread> read_treads;
      for (int j = 0; j < block_num; j++)
      {
        int block_idx = block_idxs[j];
        if (block_idx < k)
        {
          std::string block_key = keys_nodes[j].first;
          std::pair<std::string, int> &ip_and_port = keys_nodes[j].second;
          read_treads.push_back(std::thread(getFromNode, block_idx, block_key, block_size, ip_and_port.first, ip_and_port.second));
        }
      }
      for (int j = 0; j < k; j++)
      {
        read_treads[j].join();
      }

      if (IF_TEST_THROUGHPUT)
      {
        // simulate cross-rack transfer
        int cross_rack_num = 0;
        int idx = 0;
        for (int j = 0; j < block_num; j++)
        {
          if (block_idxs[j] < k)
          {
            std::pair<std::string, int> &ip_and_port = keys_nodes[j].second;
            std::string ip_port = ip_and_port.first + ":" + std::to_string(ip_and_port.second);
            int t_rack_id = m_datanode2rack[ip_port];
            if (t_rack_id != m_self_rack_id)
            {
              cross_rack_num++;
            }
          }
        }
        if (cross_rack_num > 0)
        {
          std::string temp_key = "temp";
          int val_len = block_size * cross_rack_num;
          std::string temp_value = generate_string(val_len);
          TransferToNetworkCore(temp_key.c_str(), temp_value.c_str(), val_len, false, idx);
        }
      }

      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_rack_id << "][GET]"
                  << "ready to decode!" << std::endl;
      }
      for (int j = 0; j < int(blocks_idx_ptr->size()); j++)
      {
        int idx = (*blocks_idx_ptr)[j];
        memcpy(data[idx], (*blocks_ptr)[j].data(), block_size);
      }

      std::string value;
      for (int j = 0; j < k; j++)
      {
        value += std::string(data[j]);
      }

      if (IF_DEBUG)
      {
        std::cout << "\033[1;31m[Proxy" << m_self_rack_id << "][GET]"
                  << "send " << key << " to client with length of " << value.size() << "\033[0m" << std::endl;
      }

      // send to the client
      asio::error_code error;
      asio::ip::tcp::resolver resolver(io_context);
      asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(clientip, std::to_string(clientport));
      asio::ip::tcp::socket sock_data(io_context);
      asio::connect(sock_data, endpoints);

      asio::write(sock_data, asio::buffer(key, key.size()), error);
      asio::write(sock_data, asio::buffer(value, value_size_bytes), error);
      asio::error_code ignore_ec;
      sock_data.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
      sock_data.close(ignore_ec);
    };
    try
    {
      // std::cerr << "decode_and_get_thread start" << std::endl;
      if (IF_DEBUG)
      {
        std::cout << "[Proxy] Handle get and decode" << std::endl;
      }
      std::thread my_thread(decode_and_get);
      my_thread.detach();
      // std::cerr << "decode_and_get_thread detach" << std::endl;
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }

  // delete
  grpc::Status ProxyImpl::deleteBlock(
      grpc::ServerContext *context,
      const proxy_proto::NodeAndBlock *node_and_block,
      proxy_proto::DelReply *response)
  {
    std::vector<std::string> blocks_id;
    std::vector<std::string> nodes_ip_port;
    std::string key = node_and_block->key();
    int stripe_id = node_and_block->stripe_id();
    for (int i = 0; i < node_and_block->blockkeys_size(); i++)
    {
      blocks_id.push_back(node_and_block->blockkeys(i));
      std::string ip_port = node_and_block->datanodeip(i) + ":" + std::to_string(node_and_block->datanodeport(i));
      nodes_ip_port.push_back(ip_port);
    }
    auto delete_blocks = [this, key, blocks_id, stripe_id, nodes_ip_port]() mutable
    {
      auto request_and_delete = [this](std::string block_key, std::string node_ip_port)
      {
        bool ret = DelInDatanode(block_key, node_ip_port);
        if (!ret)
        {
          std::cout << "Delete value no return!" << std::endl;
          return;
        }
      };
      try
      {
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_rack_id << "][DELETE] Handle delete " << int(blocks_id.size()) << " blocks!" << std::endl;
        }
        std::vector<std::thread> senders;
        for (int j = 0; j < int(blocks_id.size()); j++)
        {
          senders.push_back(std::thread(request_and_delete, blocks_id[j], nodes_ip_port[j]));
        }

        for (int j = 0; j < int(senders.size()); j++)
        {
          senders[j].join();
        }

        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_rack_id << "][DELETE] Delete success!" << std::endl;
        }

        if (stripe_id != -1 || key != "")
        {
          grpc::ClientContext c_context;
          coordinator_proto::CommitAbortKey commit_abort_key;
          coordinator_proto::ReplyFromCoordinator rep;
          ECProject::OpperateType opp = DEL;
          commit_abort_key.set_opp(opp);
          commit_abort_key.set_key(key);
          commit_abort_key.set_ifcommitmetadata(true);
          commit_abort_key.set_stripe_id(stripe_id);
          grpc::Status stat;
          stat = m_coordinator_ptr->reportCommitAbort(&c_context, commit_abort_key, &rep);
        }
      }
      catch (const std::exception &e)
      {
        std::cout << "exception" << std::endl;
        std::cerr << e.what() << '\n';
      }
    };
    try
    {
      std::thread my_thread(delete_blocks);
      my_thread.detach();
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }

    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::mainRecal(
      grpc::ServerContext *context,
      const proxy_proto::mainRecalPlan *main_recal_plan,
      proxy_proto::RecalReply *response)
  {
    int g_m, group_id, new_parity_num;
    bool if_partial_decoding;
    bool if_g_recal = main_recal_plan->type();
    int block_size = main_recal_plan->block_size();
    int stripe_id = main_recal_plan->stripe_id();
    int k = main_recal_plan->k();
    ECProject::EncodeType encode_type = (ECProject::EncodeType)main_recal_plan->encodetype();
    std::string recal_type = "";
    // for parity blocks
    std::vector<std::string> p_datanode_ip;
    std::vector<int> p_datanode_port;
    std::vector<std::string> p_blockkeys;
    // for help racks
    std::vector<proxy_proto::locationInfo> help_locations;
    // for blocks in local datanodes
    std::vector<std::string> l_datanode_ip;
    std::vector<int> l_datanode_port;
    std::vector<std::string> l_blockkeys;
    std::vector<int> l_blockids;
    // get the meta information
    for (int i = 0; i < main_recal_plan->p_blockkeys_size(); i++)
    {
      p_datanode_ip.push_back(main_recal_plan->p_datanodeip(i));
      p_datanode_port.push_back(main_recal_plan->p_datanodeport(i));
      p_blockkeys.push_back(main_recal_plan->p_blockkeys(i));
    }
    if_partial_decoding = main_recal_plan->if_partial_decoding();
    if (!if_g_recal)
    {
      m_mutex.lock();
      m_merge_step_processing[1] = true;
      m_mutex.unlock();
      group_id = main_recal_plan->group_id();
      new_parity_num = 1;
      recal_type = "[Local]";
    }
    else if (if_g_recal)
    {
      m_mutex.lock();
      m_merge_step_processing[0] = true;
      m_mutex.unlock();
      g_m = main_recal_plan->g_m();
      new_parity_num = g_m;
      recal_type = "[Global]";
    }
    for (int i = 0; i < main_recal_plan->racks_size(); i++)
    {
      if (int(main_recal_plan->racks(i).rack_id()) != m_self_rack_id)
      {
        proxy_proto::locationInfo temp;
        temp.set_rack_id(main_recal_plan->racks(i).rack_id());
        temp.set_proxy_ip(main_recal_plan->racks(i).proxy_ip());
        temp.set_proxy_port(main_recal_plan->racks(i).proxy_port());
        for (int j = 0; j < main_recal_plan->racks(i).blockkeys_size(); j++)
        {
          temp.add_blockids(main_recal_plan->racks(i).blockids(j));
          temp.add_blockkeys(main_recal_plan->racks(i).blockkeys(j));
          temp.add_datanodeip(main_recal_plan->racks(i).datanodeip(j));
          temp.add_datanodeport(main_recal_plan->racks(i).datanodeport(j));
        }
        help_locations.push_back(temp);
      }
      else
      {
        for (int j = 0; j < main_recal_plan->racks(i).blockkeys_size(); j++)
        {
          l_blockids.push_back(main_recal_plan->racks(i).blockids(j));
          l_blockkeys.push_back(main_recal_plan->racks(i).blockkeys(j));
          l_datanode_ip.push_back(main_recal_plan->racks(i).datanodeip(j));
          l_datanode_port.push_back(main_recal_plan->racks(i).datanodeport(j));
        }
      }
    }

    try
    {
      auto lock_ptr = std::make_shared<std::mutex>();
      auto blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
      auto blocks_idx_ptr = std::make_shared<std::vector<int>>();
      auto getFromNode = [this, blocks_ptr, blocks_idx_ptr, lock_ptr](int block_idx, std::string block_key, int block_size, std::string node_ip, int node_port) mutable
      {
        std::vector<char> temp(block_size);
        bool ret = GetFromDatanode(block_key.c_str(), block_key.size(), temp.data(), block_size, node_ip.c_str(), node_port, block_idx + 2);
        if (!ret)
        {
          std::cout << "getFromNode !ret" << std::endl;
          return;
        }
        lock_ptr->lock();
        blocks_ptr->push_back(temp);
        blocks_idx_ptr->push_back(block_idx);
        lock_ptr->unlock();
      };

      auto p_lock_ptr = std::make_shared<std::mutex>();
      auto m_blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
      auto m_blocks_idx_ptr = std::make_shared<std::vector<int>>();
      auto h_blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
      auto h_blocks_idx_ptr = std::make_shared<std::vector<int>>();
      auto getFromProxy = [this, recal_type, p_lock_ptr, m_blocks_ptr, m_blocks_idx_ptr, h_blocks_ptr, h_blocks_idx_ptr, block_size, if_partial_decoding, new_parity_num](std::shared_ptr<asio::ip::tcp::socket> socket_ptr) mutable
      {
        try
        {
          asio::error_code ec;
          std::vector<unsigned char> int_buf(sizeof(int));
          asio::read(*socket_ptr, asio::buffer(int_buf, int_buf.size()), ec);
          int t_rack_id = ECProject::bytes_to_int(int_buf);
          std::vector<unsigned char> int_flag_buf(sizeof(int));
          asio::read(*socket_ptr, asio::buffer(int_flag_buf, int_flag_buf.size()), ec);
          int t_flag = ECProject::bytes_to_int(int_flag_buf);
          std::string msg = "data";
          if (t_flag)
            msg = "partial";
          if (IF_DEBUG)
          {
            std::cout << "\033[1;36m" << "[Main Proxy " << m_self_rack_id << "] Try to get " << msg << " blocks from the proxy in rack " << t_rack_id << ". " << t_flag << "\033[0m" << std::endl;
          }
          if (t_flag)
          {
            p_lock_ptr->lock();
            for (int j = 0; j < new_parity_num; j++)
            {
              std::vector<char> tmp_val(block_size);
              asio::read(*socket_ptr, asio::buffer(tmp_val.data(), block_size), ec);
              m_blocks_ptr->push_back(tmp_val);
            }
            m_blocks_idx_ptr->push_back(t_rack_id);
            p_lock_ptr->unlock();
          }
          else
          {
            std::vector<unsigned char> int_buf_num_of_blocks(sizeof(int));
            asio::read(*socket_ptr, asio::buffer(int_buf_num_of_blocks, int_buf_num_of_blocks.size()), ec);
            int block_num = ECProject::bytes_to_int(int_buf_num_of_blocks);
            for (int j = 0; j < block_num; j++)
            {
              std::vector<char> tmp_val(block_size);
              std::vector<unsigned char> byte_block_id(sizeof(int));
              asio::read(*socket_ptr, asio::buffer(byte_block_id, byte_block_id.size()), ec);
              int block_idx = ECProject::bytes_to_int(byte_block_id);
              asio::read(*socket_ptr, asio::buffer(tmp_val.data(), block_size), ec);
              p_lock_ptr->lock();
              h_blocks_ptr->push_back(tmp_val);
              h_blocks_idx_ptr->push_back(block_idx);
              p_lock_ptr->unlock();
            }
          }

          if (IF_DEBUG)
          {
            std::cout << "\033[1;36m" << recal_type << "[Main Proxy " << m_self_rack_id << "] Finish getting data from the proxy in rack " << t_rack_id << "\033[0m" << std::endl;
          }
        }
        catch (const std::exception &e)
        {
          std::cerr << e.what() << '\n';
        }
      };

      auto send_to_datanode = [this](int j, std::string block_key, char *data, int block_size, std::string s_node_ip, int s_node_port)
      {
        SetToDatanode(block_key.c_str(), block_key.size(), data, block_size, s_node_ip.c_str(), s_node_port, j + 2);
      };

      if (IF_DEBUG)
      {
        std::cout << recal_type << "[Main Proxy" << m_self_rack_id << "] get blocks in local rack!" << std::endl;
      }
      // get data blocks in local rack
      gettimeofday(&start_time, NULL);
      int l_block_num = int(l_blockkeys.size());
      if (l_block_num > 0)
      {
        try
        {
          int cross_rack_num = 0;
          std::vector<std::thread> read_threads;
          for (int j = 0; j < l_block_num; j++)
          {
            std::string node_ip_port = l_datanode_ip[j] + ":" + std::to_string(l_datanode_port[j]);
            int t_rack_id = m_datanode2rack[node_ip_port];
            if (t_rack_id != m_self_rack_id)
            {
              cross_rack_num++;
            }
            read_threads.push_back(std::thread(getFromNode, j, l_blockkeys[j], block_size, l_datanode_ip[j], l_datanode_port[j]));
          }
          for (int j = 0; j < l_block_num; j++)
          {
            read_threads[j].join();
          }
        }
        catch (const std::exception &e)
        {
          std::cerr << e.what() << '\n';
        }
        if (l_block_num != int(blocks_ptr->size()))
        {
          std::cout << "[Help] can't get enough blocks!" << std::endl;
        }
        for (int j = 0; j < l_block_num; j++)
        {
          p_lock_ptr->lock();
          h_blocks_idx_ptr->push_back((*blocks_idx_ptr)[j]);
          h_blocks_ptr->push_back((*blocks_ptr)[j]);
          p_lock_ptr->unlock();
        }
      }
      gettimeofday(&end_time, NULL);
      inner_rack_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

      // get from proxy
      int m_num = int(help_locations.size());
      int p_num = 0;
      if (IF_DEBUG)
      {
        std::cout << recal_type << "[Main Proxy" << m_self_rack_id << "] get data blocks from " << m_num << " helper proxy!" << std::endl;
      }
      try
      {
        gettimeofday(&start_time, NULL);
        std::vector<std::thread> read_p_threads;
        for (int j = 0; j < m_num; j++)
        {
          int t_blocks_num = help_locations[j].blockkeys_size();
          bool t_flag = true;
          if (t_blocks_num <= new_parity_num)
          {
            t_flag = false;
          }
          t_flag = (if_partial_decoding && t_flag);
          if (t_flag)
          {
            p_num += 1;
            t_blocks_num = new_parity_num;
          }
          std::shared_ptr<asio::ip::tcp::socket> socket_ptr = std::make_shared<asio::ip::tcp::socket>(io_context);
          acceptor.accept(*socket_ptr);
          read_p_threads.push_back(std::thread(getFromProxy, socket_ptr));
          if (!t_flag)
          {
            l_block_num += t_blocks_num;
          }
          if (IF_DEBUG)
          {
            std::cout << recal_type << "[Main Proxy" << m_self_rack_id << "] rack" << help_locations[j].rack_id() << " blocks_num:" << help_locations[j].blockkeys_size() << std::endl;
          }
        }
        for (int j = 0; j < m_num; j++)
        {
          read_p_threads[j].join();
        }

        // simulate cross-rack transfer
        int idx = 0;
        for (int j = 0; j < m_num; j++)
        {
          int t_blocks_num = help_locations[j].blockkeys_size();
          std::string t_key = help_locations[j].blockkeys(0);
          bool t_flag = true;
          if (t_blocks_num <= new_parity_num)
          {
            t_flag = false;
          }
          t_flag = (if_partial_decoding && t_flag);
          if (t_flag)
          {
            t_blocks_num = new_parity_num;
          }
          size_t t_value_length = block_size * t_blocks_num;
          std::string temp_value = generate_string(t_value_length);
          TransferToNetworkCore(t_key.c_str(), temp_value.c_str(), t_value_length, false, idx);
        }
        gettimeofday(&end_time, NULL);
        cross_rack_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
      }
      catch (const std::exception &e)
      {
        std::cerr << e.what() << '\n';
      }

      double t_time = 0.0;
      std::string et = "(Azure_LRC)";

      int dp_block_num = int(h_blocks_idx_ptr->size());
      if (dp_block_num > 0 && if_partial_decoding)
      {
        std::vector<char *> v_data(dp_block_num);
        std::vector<char *> v_coding(new_parity_num);
        char **data = (char **)v_data.data();
        char **coding = (char **)v_coding.data();
        std::vector<std::vector<char>> v_data_area(dp_block_num, std::vector<char>(block_size));
        for (int j = 0; j < dp_block_num; j++)
        {
          data[j] = v_data_area[j].data();
        }
        for (int j = 0; j < dp_block_num; j++)
        {
          memcpy(data[j], (*h_blocks_ptr)[j].data(), block_size);
        }
        std::vector<std::vector<char>> v_coding_area(new_parity_num, std::vector<char>(block_size));
        for (int j = 0; j < new_parity_num; j++)
        {
          coding[j] = v_coding_area[j].data();
        }
        gettimeofday(&start_time, NULL);
        if (if_g_recal)
        {
          encode_partial_blocks(k, new_parity_num, data, coding, block_size, h_blocks_idx_ptr, dp_block_num, encode_type);
        }
        else
        {
          perform_addition(data, coding, block_size, dp_block_num, new_parity_num);
        }
        gettimeofday(&end_time, NULL);
        t_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

        p_lock_ptr->lock();
        for (int j = 0; j < new_parity_num; j++)
        {
          m_blocks_ptr->push_back(v_coding_area[j]);
        }
        m_blocks_idx_ptr->push_back(m_self_rack_id);
        p_lock_ptr->unlock();

        p_num += 1;
      }

      if (IF_DEBUG)
      {
        std::cout << recal_type << "[Main Proxy" << m_self_rack_id << "] recalculating new parity blocks!" << std::endl;
      }
      // encode
      int count = l_block_num;
      if (if_partial_decoding)
      {
        count = p_num * new_parity_num;
      }
      std::vector<char *> vt_data(count);
      std::vector<char *> vt_coding(new_parity_num);
      char **t_data = (char **)vt_data.data();
      char **t_coding = (char **)vt_coding.data();
      std::vector<std::vector<char>> vt_data_area(count, std::vector<char>(block_size));
      std::vector<std::vector<char>> vt_coding_area(new_parity_num, std::vector<char>(block_size));
      if (IF_DEBUG)
      {
        std::cout << recal_type << "[Main Proxy" << m_self_rack_id << "] " << count << " " << m_blocks_ptr->size() << " " << h_blocks_ptr->size() << std::endl;
      }
      for (int j = 0; j < count; j++)
      {
        t_data[j] = vt_data_area[j].data();
      }
      for (int j = 0; j < new_parity_num; j++)
      {
        t_coding[j] = vt_coding_area[j].data();
      }
      if (if_partial_decoding)
      {
        for (int j = 0; j < count; j++)
        {
          memcpy(t_data[j], (*m_blocks_ptr)[j].data(), block_size);
        }
      }
      else
      {
        for (int j = 0; j < count; j++)
        {
          memcpy(t_data[j], (*h_blocks_ptr)[j].data(), block_size);
        }
      }
      // clear
      blocks_ptr->clear();
      blocks_idx_ptr->clear();
      m_blocks_ptr->clear();
      h_blocks_idx_ptr->clear();
      m_blocks_idx_ptr->clear();
      h_blocks_ptr->clear();

      if (IF_DEBUG)
      {
        std::cout << recal_type << "[Main Proxy" << m_self_rack_id << "] encoding!" << std::endl;
      }
      try
      {
        gettimeofday(&start_time, NULL);
        if (if_partial_decoding || !if_g_recal)
        {
          perform_addition(t_data, t_coding, block_size, count, new_parity_num);
        }
        else
        {
          encode_partial_blocks(k, g_m, t_data, t_coding, block_size, h_blocks_idx_ptr, count, encode_type);
        }
        gettimeofday(&end_time, NULL);
        t_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
        encoding_time = t_time;
        std::cout << et << "Main Recalculation Time: " << t_time << " (block_size=" << block_size / (1024 * 1024) << "MB)." << std::endl;
      }

      catch (const std::exception &e)
      {
        std::cerr << e.what() << '\n';
      }

      // set
      if (IF_DEBUG)
      {
        std::cout << recal_type << "[Main Proxy" << m_self_rack_id << "] set new parity blocks!" << std::endl;
      }
      try
      {
        gettimeofday(&start_time, NULL);
        int cross_rack_num = 0;
        std::vector<std::thread> set_threads;
        for (int i = 0; i < new_parity_num; i++)
        {
          std::string new_id = "";
          if (if_g_recal)
          {
            if (encode_type == RS)
            {
              new_id = "Stripe" + std::to_string(stripe_id) + "_P" + std::to_string(i);
            }
            else
            {
              new_id = "Stripe" + std::to_string(stripe_id) + "_G" + std::to_string(i);
            }
          }
          else
          {
            new_id = "Stripe" + std::to_string(stripe_id) + "_L" + std::to_string(group_id);
          }
          std::string s_node_ip = p_datanode_ip[i];
          int s_node_port = p_datanode_port[i];
          if (IF_DEBUG)
          {
            std::cout << recal_type << "[Main Proxy" << m_self_rack_id << "] set " << new_id << " to datanode " << s_node_port << std::endl;
          }
          std::string node_ip_port = s_node_ip + ":" + std::to_string(s_node_port);
          int t_rack_id = m_datanode2rack[node_ip_port];
          if (t_rack_id != m_self_rack_id)
          {
            cross_rack_num++;
          }
          set_threads.push_back(std::thread(send_to_datanode, i, new_id, t_coding[i], block_size, s_node_ip, s_node_port));
        }
        for (int i = 0; i < new_parity_num; i++)
        {
          set_threads[i].join();
        }
        gettimeofday(&end_time, NULL);
        inner_rack_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

        gettimeofday(&start_time, NULL);
        // simulate cross-rack transfer
        if (cross_rack_num > 0)
        {
          std::string t_key = "Stripe_GorL";
          size_t t_value_length = block_size * cross_rack_num;
          std::string temp_value = generate_string(t_value_length);
          TransferToNetworkCore(t_key.c_str(), temp_value.c_str(), t_value_length, false, 0);
        }
        gettimeofday(&end_time, NULL);
        cross_rack_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
      }
      catch (const std::exception &e)
      {
        std::cerr << e.what() << '\n';
      }

      if (if_g_recal)
      {
        m_merge_step_processing[0] = false;
        cv.notify_all();
      }
      else
      {
        m_merge_step_processing[1] = false;
        cv.notify_all();
      }
    }
    catch (const std::exception &e)
    {
      std::cout << "[Proxy" << m_self_rack_id << "] error!" << std::endl;
      std::cerr << e.what() << '\n';
    }

    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::helpRecal(
      grpc::ServerContext *context,
      const proxy_proto::helpRecalPlan *help_recal_plan,
      proxy_proto::RecalReply *response)
  {

    bool if_partial_decoding = help_recal_plan->if_partial_decoding();
    bool if_g_recal = help_recal_plan->type();
    std::string proxy_ip = help_recal_plan->mainproxyip();
    int proxy_port = help_recal_plan->mainproxyport();
    int block_size = help_recal_plan->block_size();
    int parity_num = help_recal_plan->parity_num();
    int k = help_recal_plan->k();
    ECProject::EncodeType encode_type = (ECProject::EncodeType)help_recal_plan->encodetype();
    std::vector<std::string> datanode_ip;
    std::vector<int> datanode_port;
    std::vector<std::string> blockkeys;
    std::vector<int> blockids;
    for (int i = 0; i < help_recal_plan->blockkeys_size(); i++)
    {
      datanode_ip.push_back(help_recal_plan->datanodeip(i));
      datanode_port.push_back(help_recal_plan->datanodeport(i));
      blockkeys.push_back(help_recal_plan->blockkeys(i));
      blockids.push_back(help_recal_plan->blockids(i));
    }

    bool flag = true;
    if (int(blockkeys.size()) <= parity_num)
    {
      flag = false;
    }
    if_partial_decoding = (if_partial_decoding && flag);

    // get data from the datanode
    auto myLock_ptr = std::make_shared<std::mutex>();
    auto blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
    auto blocks_idx_ptr = std::make_shared<std::vector<int>>();
    auto getFromNode = [this, blocks_ptr, blocks_idx_ptr, myLock_ptr](int block_idx, std::string block_key, int block_size, std::string node_ip, int node_port) mutable
    {
      std::vector<char> temp(block_size);
      bool ret = GetFromDatanode(block_key.c_str(), block_key.size(), temp.data(), block_size, node_ip.c_str(), node_port, block_idx + 2);

      if (!ret)
      {
        std::cout << "getFromNode !ret" << std::endl;
        return;
      }
      myLock_ptr->lock();
      blocks_ptr->push_back(temp);
      blocks_idx_ptr->push_back(block_idx);
      myLock_ptr->unlock();
    };
    if (IF_DEBUG)
    {
      std::cout << "[Helper Proxy" << m_self_rack_id << "] Ready to read blocks from data node!" << std::endl;
    }
    int block_num = int(blockkeys.size());

    try
    {
      int cross_rack_num = 0;
      std::vector<std::thread> read_treads;
      for (int j = 0; j < block_num; j++)
      {
        std::string node_ip_port = datanode_ip[j] + ":" + std::to_string(datanode_port[j]);
        int t_rack_id = m_datanode2rack[node_ip_port];
        if (t_rack_id != m_self_rack_id)
        {
          cross_rack_num++;
        }
        read_treads.push_back(std::thread(getFromNode, blockids[j], blockkeys[j], block_size, datanode_ip[j], datanode_port[j]));
      }
      for (int j = 0; j < block_num; j++)
      {
        read_treads[j].join();
      }
      // simulate cross-rack transfer
      if (cross_rack_num > 0 && if_partial_decoding)
      {
        std::string t_key = blockkeys[0];
        size_t t_value_length = block_size * cross_rack_num;
        std::string temp_value = generate_string(t_value_length);
        TransferToNetworkCore(t_key.c_str(), temp_value.c_str(), t_value_length, false, 0);
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }
    if (block_num != int(blocks_ptr->size()))
    {
      std::cout << "[Help] can't get enough blocks!" << std::endl;
    }

    std::vector<char *> v_data(block_num);
    std::vector<char *> v_coding(parity_num);
    char **data = (char **)v_data.data();
    char **coding = (char **)v_coding.data();
    std::vector<std::vector<char>> v_data_area(block_num, std::vector<char>(block_size));
    std::vector<std::vector<char>> v_coding_area(parity_num, std::vector<char>(block_size));
    for (int j = 0; j < block_num; j++)
    {
      data[j] = v_data_area[j].data();
    }
    for (int j = 0; j < parity_num; j++)
    {
      coding[j] = v_coding_area[j].data();
    }
    for (int j = 0; j < block_num; j++)
    {
      memcpy(data[j], (*blocks_ptr)[j].data(), block_size);
    }

    // encode
    if (if_partial_decoding) // partial encoding
    {
      if (IF_DEBUG)
      {
        std::cout << "[Helper Proxy" << m_self_rack_id << "] partial encoding!" << std::endl;
        for (auto it = blocks_idx_ptr->begin(); it != blocks_idx_ptr->end(); it++)
        {
          std::cout << (*it) << " ";
        }
        std::cout << std::endl;
      }

      double t_time = 0.0;
      std::string et = "(Azure_LRC)";
      gettimeofday(&start_time, NULL);
      if (if_g_recal)
      {
        encode_partial_blocks(k, parity_num, data, coding, block_size, blocks_idx_ptr, block_num, encode_type);
      }
      else
      {
        perform_addition(data, coding, block_size, block_num, parity_num);
      }
      gettimeofday(&end_time, NULL);
      t_time = end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
      std::cout << et << "Help Encode Time: " << t_time << " (block_size=" << block_size / (1024 * 1024) << "MB)." << std::endl;
      response->set_encoding_time(t_time);
    }

    // send to main proxy
    asio::error_code error;
    asio::io_context io_context;
    asio::ip::tcp::socket socket(io_context);
    asio::ip::tcp::resolver resolver(io_context);
    asio::error_code con_error;
    if (IF_DEBUG)
    {
      std::cout << "\033[1;36m[Helper Proxy" << m_self_rack_id << "] Try to connect main proxy port " << proxy_port << "\033[0m" << std::endl;
    }
    asio::connect(socket, resolver.resolve({proxy_ip, std::to_string(proxy_port)}), con_error);
    if (!con_error && IF_DEBUG)
    {
      std::cout << "Connect to " << proxy_ip << ":" << proxy_port << " success!" << std::endl;
    }

    int value_size = 0;

    std::vector<unsigned char> int_buf_self_rack_id = ECProject::int_to_bytes(m_self_rack_id);
    asio::write(socket, asio::buffer(int_buf_self_rack_id, int_buf_self_rack_id.size()), error);
    if (!if_partial_decoding)
    {
      std::vector<unsigned char> t_flag = ECProject::int_to_bytes(0);
      asio::write(socket, asio::buffer(t_flag, t_flag.size()), error);
      std::vector<unsigned char> int_buf_num_of_blocks = ECProject::int_to_bytes(block_num);
      asio::write(socket, asio::buffer(int_buf_num_of_blocks, int_buf_num_of_blocks.size()), error);
      int j = 0;
      for (auto it = blocks_idx_ptr->begin(); it != blocks_idx_ptr->end(); it++, j++)
      {
        // send index and value
        int block_idx = *it;
        std::vector<unsigned char> byte_block_idx = ECProject::int_to_bytes(block_idx);
        asio::write(socket, asio::buffer(byte_block_idx, byte_block_idx.size()), error);
        asio::write(socket, asio::buffer(data[j], block_size), error);
        value_size += block_size;
      }
    }
    else
    {
      std::vector<unsigned char> t_flag = ECProject::int_to_bytes(1);
      asio::write(socket, asio::buffer(t_flag, t_flag.size()), error);
      for (int j = 0; j < parity_num; j++)
      {
        asio::write(socket, asio::buffer(coding[j], block_size), error);
        value_size += block_size;
      }
    }
    asio::error_code ignore_ec;
    socket.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
    socket.close(ignore_ec);
    if (IF_DEBUG)
    {
      std::cout << "[Helper Proxy" << m_self_rack_id << "] Send value to proxy" << proxy_port << "! With length of " << value_size << std::endl;
    }

    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::handleMergeHPC(
      grpc::ServerContext *context,
      const proxy_proto::MergePlanHPC *merge_plan,
      proxy_proto::RecalReply *response)
  {
    int block_size = merge_plan->block_size();
    // bool isvertical = merge_plan->isvertical();
    int x = merge_plan->x();
    // int stripe_id = merge_plan->stripe_id();

    m_mutex.lock();
    m_merge_step_processing[0] = true;
    m_mutex.unlock();

    // for old parities
    std::vector<std::string> o_datanode_ip;
    std::vector<int> o_datanode_port;
    std::vector<std::string> o_blockkeys;
    std::vector<int> o_blockidxs;
    std::unordered_set<int> t_idx_set;
    int o_block_num = int(merge_plan->blockkeys_size());
    for (int i = 0; i < o_block_num; i++)
    {
      o_datanode_ip.push_back(merge_plan->datanodeip(i));
      o_datanode_port.push_back(merge_plan->datanodeport(i));
      o_blockkeys.push_back(merge_plan->blockkeys(i));
      o_blockidxs.push_back(merge_plan->blockidxs(i));
      t_idx_set.insert(merge_plan->blockidxs(i));
    }
    // for new parities
    std::vector<std::string> n_datanode_ip;
    std::vector<int> n_datanode_port;
    std::vector<std::string> n_blockkeys;
    std::vector<int> n_blockidxs;
    int n_block_num = int(merge_plan->n_blockkeys_size());
    for (int i = 0; i < n_block_num; i++)
    {
      n_datanode_ip.push_back(merge_plan->n_datanodeip(i));
      n_datanode_port.push_back(merge_plan->n_datanodeport(i));
      n_blockkeys.push_back(merge_plan->n_blockkeys(i));
      n_blockidxs.push_back(merge_plan->n_blockidxs(i));
    }
    try
    {
      auto lock_ptr = std::make_shared<std::mutex>();
      auto blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
      auto blocks_idx_ptr = std::make_shared<std::vector<int>>();
      auto getFromNode = [this, blocks_ptr, blocks_idx_ptr, lock_ptr, block_size](int block_idx, std::string block_key, std::string node_ip, int node_port) mutable
      {
        std::vector<char> temp(block_size);
        bool ret = GetFromDatanode(block_key.c_str(), block_key.size(), temp.data(), block_size, node_ip.c_str(), node_port, 2);
        if (!ret)
        {
          std::cout << "getFromNode! Failed!" << std::endl;
          return;
        }
        lock_ptr->lock();
        blocks_ptr->push_back(temp);
        blocks_idx_ptr->push_back(block_idx);
        lock_ptr->unlock();
      };
      auto send_to_datanode = [this](std::string block_key, char *data, int block_size, std::string s_node_ip, int s_node_port)
      {
        SetToDatanode(block_key.c_str(), block_key.size(), data, block_size, s_node_ip.c_str(), s_node_port, 2);
      };
      try
      {
        std::vector<std::thread> read_threads;
        for (int i = 0; i < o_block_num; i++)
        {
          read_threads.push_back(std::thread(getFromNode, o_blockidxs[i], o_blockkeys[i], o_datanode_ip[i], o_datanode_port[i]));
        }
        for (int i = 0; i < o_block_num; i++)
        {
          read_threads[i].join();
        }
      }
      catch (const std::exception &e)
      {
        std::cerr << e.what() << '\n';
      }

      std::vector<char *> v_data(o_block_num);
      char **o_data = (char **)v_data.data();
      std::vector<std::vector<char>> v_data_area(o_block_num, std::vector<char>(block_size));
      for (int i = 0; i < o_block_num; i++)
      {
        o_data[i] = v_data_area[i].data();
        memcpy(o_data[i], (*blocks_ptr)[i].data(), block_size);
      }
      if (IF_DEBUG)
      {
        std::cout << "[Proxy" << m_self_rack_id << "] Ready to encode! " << std::endl;
      }
      double t_time = 0.0;
      std::string et = "(HPC)";
      for (auto it = t_idx_set.begin(); it != t_idx_set.end(); it++)
      {
        int t_idx = *it;
        std::vector<char *> t_data(x);
        char **data = (char **)t_data.data();
        std::vector<char *> t_coding(1);
        char **coding = (char **)t_coding.data();
        std::vector<std::vector<char>> t_coding_area(1, std::vector<char>(block_size));
        coding[0] = t_coding_area[0].data();
        int cnt = 0;
        for (int i = 0; i < o_block_num; i++)
        {
          if (t_idx == o_blockidxs[i])
          {
            data[cnt++] = o_data[i];
          }
        }
        if (cnt != x)
        {
          std::cout << "Error! Numbers not matches! cnt = " << cnt << std::endl;
        }
        int n_idx = -1;
        for (int i = 0; i < n_block_num; i++)
        {
          if (t_idx == n_blockidxs[i])
          {
            n_idx = i;
            break;
          }
        }
        if (n_idx < 0)
        {
          std::cout << "Error! New block index error!" << std::endl;
        }

        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_rack_id << "] encoding! " << std::endl;
        }
        gettimeofday(&start_time, NULL);
        perform_addition_xor(data, coding, block_size, cnt, 1);
        gettimeofday(&end_time, NULL);
        t_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
        encoding_time = t_time;
        if (IF_DEBUG)
        {
          std::cout << "[Proxy" << m_self_rack_id << "] set " << n_blockkeys[n_idx] << " to datanode " << n_datanode_port[n_idx] << std::endl;
        }
        std::thread send_thread(send_to_datanode, n_blockkeys[n_idx], coding[0], block_size, n_datanode_ip[n_idx], n_datanode_port[n_idx]);
        send_thread.join();
      }
      std::cout << et << "Recalculation Time: " << t_time << " (block_size=" << block_size / (1024 * 1024) << "MB)." << std::endl;
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }
    m_merge_step_processing[0] = false;
    cv.notify_all();

    return grpc::Status::OK;
  }

  // repair
  grpc::Status ProxyImpl::mainRepair(
      grpc::ServerContext *context,
      const proxy_proto::mainRepairPlan *main_repair_plan,
      proxy_proto::RepairReply *response)
  {
    bool if_partial_decoding = main_repair_plan->if_partial_decoding();
    // bool approach = main_repair_plan->approach();
    int k = main_repair_plan->k();
    int m_g = main_repair_plan->m_g();
    int x_l = main_repair_plan->x_l();
    int seri_num = main_repair_plan->seri_num();
    int block_size = main_repair_plan->block_size();
    int encodetype = main_repair_plan->encodetype();
    bool rv_or_ch__isglobal = main_repair_plan->rv_or_ch__isglobal();

    // for failed blocks
    std::vector<std::string> failed_datanodeip;
    std::vector<int> failed_datanodeport;
    std::vector<std::string> failed_blockkeys;
    std::vector<int> failed_blockids;
    int failed_block_num = int(main_repair_plan->failed_blockkeys_size());
    int *erasures = new int[failed_block_num + 1];
    for (int i = 0; i < int(main_repair_plan->failed_blockkeys_size()); i++)
    {
      failed_blockkeys.push_back(main_repair_plan->failed_blockkeys(i));
      failed_blockids.push_back(main_repair_plan->failed_blockids(i));
      failed_datanodeip.push_back(main_repair_plan->failed_datanodeip(i));
      failed_datanodeport.push_back(main_repair_plan->failed_datanodeport(i));
      erasures[i] = main_repair_plan->failed_blockids(i);
    }
    erasures[failed_block_num] = -1;
    // for selected parity blocks to repair
    auto parity_idx_ptr = std::make_shared<std::vector<int>>();
    for (int i = 0; i < int(main_repair_plan->parity_blockids_size()); i++)
    {
      parity_idx_ptr->push_back(main_repair_plan->parity_blockids(i) - k);
    }
    // for help racks
    std::vector<proxy_proto::locationInfo> help_locations;
    // for blocks in local datanodes
    std::vector<std::string> l_datanode_ip;
    std::vector<int> l_datanode_port;
    std::vector<std::string> l_blockkeys;
    std::vector<int> l_blockids;
    for (int i = 0; i < int(main_repair_plan->racks_size()); i++)
    {
      if (int(main_repair_plan->racks(i).rack_id()) != m_self_rack_id)
      {
        proxy_proto::locationInfo temp;
        temp.set_rack_id(main_repair_plan->racks(i).rack_id());
        temp.set_proxy_ip(main_repair_plan->racks(i).proxy_ip());
        temp.set_proxy_port(main_repair_plan->racks(i).proxy_port());
        for (int j = 0; j < int(main_repair_plan->racks(i).blockkeys_size()); j++)
        {
          temp.add_blockkeys(main_repair_plan->racks(i).blockkeys(j));
          temp.add_blockids(main_repair_plan->racks(i).blockids(j));
          temp.add_datanodeip(main_repair_plan->racks(i).datanodeip(j));
          temp.add_datanodeport(main_repair_plan->racks(i).datanodeport(j));
        }
        help_locations.push_back(temp);
      }
      else
      {
        for (int j = 0; j < main_repair_plan->racks(i).blockkeys_size(); j++)
        {
          l_blockids.push_back(main_repair_plan->racks(i).blockids(j));
          l_blockkeys.push_back(main_repair_plan->racks(i).blockkeys(j));
          l_datanode_ip.push_back(main_repair_plan->racks(i).datanodeip(j));
          l_datanode_port.push_back(main_repair_plan->racks(i).datanodeport(j));
        }
      }
    }

    bool flag = true;
    if (int(help_locations.size()) == 0) // repair inner-rack
    {
      flag = false;
    }
    if_partial_decoding = (flag && if_partial_decoding);

    m_mutex.lock();
    m_merge_step_processing[0] = true;
    m_mutex.unlock();

    try
    {
      auto lock_ptr = std::make_shared<std::mutex>();
      auto blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
      auto blocks_idx_ptr = std::make_shared<std::vector<int>>();
      auto getFromNode = [this, blocks_ptr, blocks_idx_ptr, lock_ptr](int block_idx, std::string block_key, int block_size, std::string node_ip, int node_port) mutable
      {
        std::vector<char> temp(block_size);
        bool ret = GetFromDatanode(block_key.c_str(), block_key.size(), temp.data(), block_size, node_ip.c_str(), node_port, block_idx + 2);
        if (!ret)
        {
          std::cout << "getFromNode !ret" << std::endl;
          return;
        }
        lock_ptr->lock();
        blocks_ptr->push_back(temp);
        blocks_idx_ptr->push_back(block_idx);
        lock_ptr->unlock();
      };

      auto h_lock_ptr = std::make_shared<std::mutex>();
      auto d_blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
      auto d_blocks_idx_ptr = std::make_shared<std::vector<int>>();
      auto p_blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
      auto p_blocks_idx_ptr = std::make_shared<std::vector<int>>();
      auto getFromProxy = [this, h_lock_ptr, d_blocks_ptr, d_blocks_idx_ptr, p_blocks_ptr, p_blocks_idx_ptr, block_size, if_partial_decoding, failed_block_num](std::shared_ptr<asio::ip::tcp::socket> socket_ptr) mutable
      {
        try
        {
          asio::error_code ec;
          std::vector<unsigned char> int_buf(sizeof(int));
          asio::read(*socket_ptr, asio::buffer(int_buf, int_buf.size()), ec);
          int t_rack_id = ECProject::bytes_to_int(int_buf);
          std::vector<unsigned char> int_flag_buf(sizeof(int));
          asio::read(*socket_ptr, asio::buffer(int_flag_buf, int_flag_buf.size()), ec);
          int t_flag = ECProject::bytes_to_int(int_flag_buf);
          std::string msg = "data";
          if (t_flag)
            msg = "partial";
          if (IF_DEBUG)
          {
            std::cout << "\033[1;36m" << "[Main Proxy " << m_self_rack_id << "] Try to get " << msg << " blocks from the proxy in rack " << t_rack_id << ". " << t_flag << "\033[0m" << std::endl;
          }
          if (t_flag) // receive partial blocks from help proxies
          {
            h_lock_ptr->lock();
            for (int j = 0; j < failed_block_num; j++)
            {
              std::vector<char> tmp_val(block_size);
              asio::read(*socket_ptr, asio::buffer(tmp_val.data(), block_size), ec);
              p_blocks_ptr->push_back(tmp_val);
            }
            p_blocks_idx_ptr->push_back(t_rack_id);
            h_lock_ptr->unlock();
          }
          else // receive data blocks from help proxies
          {
            std::vector<unsigned char> int_buf_num_of_blocks(sizeof(int));
            asio::read(*socket_ptr, asio::buffer(int_buf_num_of_blocks, int_buf_num_of_blocks.size()), ec);
            int block_num = ECProject::bytes_to_int(int_buf_num_of_blocks);
            for (int j = 0; j < block_num; j++)
            {
              std::vector<char> tmp_val(block_size);
              std::vector<unsigned char> byte_block_id(sizeof(int));
              asio::read(*socket_ptr, asio::buffer(byte_block_id, byte_block_id.size()), ec);
              int block_idx = ECProject::bytes_to_int(byte_block_id);
              asio::read(*socket_ptr, asio::buffer(tmp_val.data(), block_size), ec);
              h_lock_ptr->lock();
              d_blocks_ptr->push_back(tmp_val);
              d_blocks_idx_ptr->push_back(block_idx);
              h_lock_ptr->unlock();
            }
          }

          if (IF_DEBUG)
          {
            std::cout << "\033[1;36m" << "[Main Proxy " << m_self_rack_id << "] Finish getting data from the proxy in rack " << t_rack_id << "\033[0m" << std::endl;
          }
        }
        catch (const std::exception &e)
        {
          std::cerr << e.what() << '\n';
        }
      };

      auto send_to_datanode = [this](int j, std::string block_key, char *data, int block_size, std::string s_node_ip, int s_node_port)
      {
        SetToDatanode(block_key.c_str(), block_key.size(), data, block_size, s_node_ip.c_str(), s_node_port, j + 2);
      };

      if (IF_DEBUG)
      {
        std::cout << "[Main Proxy" << m_self_rack_id << "] get blocks in local rack!" << std::endl;
      }

      gettimeofday(&start_time, NULL);
      // get data blocks in the racks
      int l_block_num = int(l_blockkeys.size());
      if (l_block_num > 0)
      {
        try
        {
          int cross_rack_num = 0;
          std::vector<std::thread> read_threads;
          for (int j = 0; j < l_block_num; j++)
          {
            std::string node_ip_port = l_datanode_ip[j] + ":" + std::to_string(l_datanode_port[j]);
            int t_rack_id = m_datanode2rack[node_ip_port];
            if (t_rack_id != m_self_rack_id)
            {
              cross_rack_num++;
            }
            read_threads.push_back(std::thread(getFromNode, j, l_blockkeys[j], block_size, l_datanode_ip[j], l_datanode_port[j]));
          }
          for (int j = 0; j < l_block_num; j++)
          {
            read_threads[j].join();
          }
          // simulate cross-rack transfer
          if (int(m_networkcore.size()) == 2 && cross_rack_num > 0)
          {
            std::string t_key = l_blockkeys[0];
            size_t t_value_length = block_size * cross_rack_num;
            std::string temp_value = generate_string(t_value_length);
            TransferToNetworkCore(t_key.c_str(), temp_value.c_str(), t_value_length, false, 0);
          }
        }
        catch (const std::exception &e)
        {
          std::cerr << e.what() << '\n';
        }
        if (l_block_num != int(blocks_ptr->size()))
        {
          std::cout << "[Main] can't get enough blocks!" << std::endl;
        }
        for (int j = 0; j < l_block_num; j++)
        {
          h_lock_ptr->lock();
          d_blocks_idx_ptr->push_back((*blocks_idx_ptr)[j]);
          d_blocks_ptr->push_back((*blocks_ptr)[j]);
          h_lock_ptr->unlock();
        }
      }
      gettimeofday(&end_time, NULL);
      inner_rack_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

      // get from proxy
      gettimeofday(&start_time, NULL);
      int m_num = int(help_locations.size());
      int p_num = 0;
      if (IF_DEBUG)
      {
        std::cout << "[Main Proxy" << m_self_rack_id << "] get data blocks from " << m_num << " helper proxy!" << std::endl;
      }
      try
      {
        std::vector<std::thread> read_p_threads;
        for (int j = 0; j < m_num; j++)
        {
          int t_blocks_num = help_locations[j].blockkeys_size();
          bool t_flag = true;
          if (t_blocks_num <= failed_block_num)
          {
            t_flag = false;
          }
          t_flag = (if_partial_decoding && t_flag);
          if (t_flag)
          {
            p_num += 1;
            t_blocks_num = failed_block_num;
          }
          std::shared_ptr<asio::ip::tcp::socket> socket_ptr = std::make_shared<asio::ip::tcp::socket>(io_context);
          acceptor.accept(*socket_ptr);
          read_p_threads.push_back(std::thread(getFromProxy, socket_ptr));
          if (!t_flag)
          {
            l_block_num += t_blocks_num;
          }
          if (IF_DEBUG)
          {
            std::cout << "[Main Proxy" << m_self_rack_id << "] rack" << help_locations[j].rack_id() << " blocks_num:" << help_locations[j].blockkeys_size() << std::endl;
          }
        }
        for (int j = 0; j < m_num; j++)
        {
          read_p_threads[j].join();
        }

        // simulate cross-rack transfer
        int idx = 0;
        for (int j = 0; j < m_num; j++)
        {
          int t_blocks_num = help_locations[j].blockkeys_size();
          bool t_flag = true;
          if (t_blocks_num <= failed_block_num)
          {
            t_flag = false;
          }
          t_flag = (if_partial_decoding && t_flag);
          if (t_flag)
          {
            t_blocks_num = failed_block_num;
          }
          std::string t_key = help_locations[j].blockkeys(0);
          size_t t_value_length = block_size * t_blocks_num;
          std::string temp_value = generate_string(t_value_length);
          TransferToNetworkCore(t_key.c_str(), temp_value.c_str(), t_value_length, false, idx);
        }
      }
      catch (const std::exception &e)
      {
        std::cerr << e.what() << '\n';
      }
      gettimeofday(&end_time, NULL);
      cross_rack_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

      double t_time = 0.0;
      std::string et = "";
      int dp_block_num = int(d_blocks_idx_ptr->size());
      if (dp_block_num > 0 && if_partial_decoding)
      {
        std::vector<char *> v_data(dp_block_num);
        std::vector<char *> v_coding(failed_block_num);
        char **data = (char **)v_data.data();
        char **coding = (char **)v_coding.data();
        std::vector<std::vector<char>> v_data_area(dp_block_num, std::vector<char>(block_size));
        for (int j = 0; j < dp_block_num; j++)
        {
          data[j] = v_data_area[j].data();
        }
        for (int j = 0; j < dp_block_num; j++)
        {
          memcpy(data[j], (*d_blocks_ptr)[j].data(), block_size);
        }
        std::vector<std::vector<char>> v_coding_area(failed_block_num, std::vector<char>(block_size));
        for (int j = 0; j < failed_block_num; j++)
        {
          coding[j] = v_coding_area[j].data();
        }
        gettimeofday(&start_time, NULL);
        if (encodetype == ECProject::HPC)
        {
          encode_partial_block_with_data_blocks_HPC(x_l, seri_num, rv_or_ch__isglobal, k, m_g, data, coding, block_size, d_blocks_idx_ptr, dp_block_num, parity_idx_ptr);
        }
        else if (encodetype == ECProject::MLEC)
        {
          encode_partial_blocks_for_repair_PC(k, m_g, data, coding, block_size, d_blocks_idx_ptr, dp_block_num, parity_idx_ptr);
        }
        else if (encodetype == ECProject::Azure_LRC)
        {
          encode_partial_blocks_for_repair_LRC(k, m_g, x_l, data, coding, block_size, d_blocks_idx_ptr, dp_block_num, parity_idx_ptr);
        }
        else
        {
          encode_partial_blocks_for_repair_RS(k, m_g, data, coding, block_size, d_blocks_idx_ptr, dp_block_num, parity_idx_ptr);
        }
        gettimeofday(&end_time, NULL);
        t_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

        h_lock_ptr->lock();
        for (int j = 0; j < failed_block_num; j++)
        {
          p_blocks_ptr->push_back(v_coding_area[j]);
        }
        p_blocks_idx_ptr->push_back(m_self_rack_id);
        h_lock_ptr->unlock();

        p_num += 1;
      }

      if (IF_DEBUG)
      {
        std::cout << "[Main Proxy" << m_self_rack_id << "] ready to decode! " << dp_block_num << " " << p_num << std::endl;
      }
      // decode
      int count = l_block_num;
      if (if_partial_decoding)
      {
        count = p_num * failed_block_num;
      }
      int coding_num = failed_block_num;
      int data_num = count;
      if (!if_partial_decoding)
      {
        data_num = k;
        if ((encodetype == ECProject::HPC || encodetype == ECProject::MLEC) && failed_block_num < m_g)
        {
          coding_num = m_g;
        }
        else if (encodetype == ECProject::Azure_LRC && failed_block_num < m_g + x_l)
        {
          coding_num = m_g + x_l;
        }
        else if (encodetype == ECProject::RS && failed_block_num < m_g)
        {
          coding_num = m_g;
        }
      }
      std::vector<char *> vt_data(data_num);
      std::vector<char *> vt_coding(coding_num);
      char **t_data = (char **)vt_data.data();
      char **t_coding = (char **)vt_coding.data();
      std::vector<std::vector<char>> vt_data_area(data_num, std::vector<char>(block_size));
      std::vector<std::vector<char>> vt_coding_area(coding_num, std::vector<char>(block_size));
      if (IF_DEBUG)
      {
        std::cout << "[Main Proxy" << m_self_rack_id << "] " << count << " " << p_blocks_ptr->size() << " " << d_blocks_ptr->size() << std::endl;
      }
      for (int j = 0; j < data_num; j++)
      {
        t_data[j] = vt_data_area[j].data();
      }
      for (int j = 0; j < coding_num; j++)
      {
        t_coding[j] = vt_coding_area[j].data();
      }
      if (if_partial_decoding) // decode with partial blocks
      {
        for (int j = 0; j < count; j++)
        {
          memcpy(t_data[j], (*p_blocks_ptr)[j].data(), block_size);
        }
      }
      else // decode with data blocks
      {
        for (int j = 0; j < count; j++)
        {
          int idx = (*d_blocks_idx_ptr)[j];
          if (idx >= k)
          {
            memcpy(t_coding[idx - k], (*d_blocks_ptr)[j].data(), block_size);
          }
          else
          {
            memcpy(t_data[idx], (*d_blocks_ptr)[j].data(), block_size);
          }
        }
      }
      // clear
      blocks_ptr->clear();
      blocks_idx_ptr->clear();
      p_blocks_ptr->clear();
      d_blocks_idx_ptr->clear();
      p_blocks_idx_ptr->clear();
      d_blocks_ptr->clear();

      if (IF_DEBUG)
      {
        std::cout << "[Main Proxy" << m_self_rack_id << "] encoding!" << std::endl;
      }
      try
      {
        gettimeofday(&start_time, NULL);
        if (encodetype == ECProject::HPC)
        {
          if (if_partial_decoding)
          {
            decode_with_partial_blocks_HPC(x_l, seri_num, rv_or_ch__isglobal, k, m_g, t_data, t_coding, block_size, count, parity_idx_ptr, erasures);
          }
          else
          {
            decode_by_row_or_col_enlarged(x_l, seri_num, rv_or_ch__isglobal, k, m_g, t_data, t_coding, block_size, erasures, failed_block_num);
          }
          et = "(HPC)";
        }
        else if (encodetype == ECProject::MLEC)
        {
          if (if_partial_decoding)
          {
            decode_with_partial_blocks_PC(k, m_g, t_data, t_coding, block_size, count, parity_idx_ptr, erasures);
          }
          else
          {
            decode_by_row_or_col(k, m_g, t_data, t_coding, block_size, erasures, failed_block_num);
          }
          et = "(MLEC)";
        }
        else if (encodetype == ECProject::Azure_LRC)
        {
          if (if_partial_decoding)
          {
            // if(rv_or_ch__isglobal)
            // {
            decode_with_partial_blocks_LRC(k, m_g, x_l, t_data, t_coding, block_size, count, parity_idx_ptr, erasures);
            // }
            // else
            // {
            //   perform_addition(t_data, t_coding, block_size, count, failed_block_num);
            // }
          }
          else
          {
            // if(rv_or_ch__isglobal)
            // {
            decode_lrc(k, m_g, x_l, t_data, t_coding, block_size, erasures, failed_block_num);
            // }
            // else
            // {
            //   perform_addition(t_data, t_coding, block_size, count, failed_block_num);
            // }
          }
          et = "(Azure_LRC)";
        }
        else
        {
          if (if_partial_decoding)
          {
            decode_with_partial_blocks_RS(k, m_g, t_data, t_coding, block_size, count, parity_idx_ptr, erasures);
          }
          else
          {
            decode_RS(k, m_g, t_data, t_coding, block_size, erasures, failed_block_num);
          }
          et = "(RS)";
        }
        gettimeofday(&end_time, NULL);
        t_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
        std::cout << et << "Main Decode Time: " << t_time << " (block_size=" << block_size / (1024 * 1024) << "MB)." << std::endl;
        decoding_time = t_time;
      }
      catch (const std::exception &e)
      {
        std::cerr << e.what() << '\n';
      }
      response->set_time(t_time);

      try
      {
        gettimeofday(&start_time, NULL);
        std::vector<std::thread> set_threads;
        for (int i = 0; i < failed_block_num; i++)
        {
          if (IF_DEBUG)
          {
            std::cout << "[Main Proxy" << m_self_rack_id << "] set " << failed_blockkeys[i] << " to datanode " << failed_datanodeport[i] << std::endl;
          }
          if (!if_partial_decoding)
          {
            int index = failed_blockids[i];
            if (index >= k)
            {
              set_threads.push_back(std::thread(send_to_datanode, i, failed_blockkeys[i], t_coding[index - k], block_size, failed_datanodeip[i], failed_datanodeport[i]));
            }
            else
            {
              set_threads.push_back(std::thread(send_to_datanode, i, failed_blockkeys[i], t_data[index], block_size, failed_datanodeip[i], failed_datanodeport[i]));
            }
          }
          else
          {
            set_threads.push_back(std::thread(send_to_datanode, i, failed_blockkeys[i], t_coding[i], block_size, failed_datanodeip[i], failed_datanodeport[i]));
          }
        }
        for (int i = 0; i < failed_block_num; i++)
        {
          set_threads[i].join();
        }
        gettimeofday(&end_time, NULL);
        inner_rack_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

        // simulate cross-rack transfer
        gettimeofday(&start_time, NULL);
        int cross_rack_num = 0;
        int idx = 0;
        for (int j = 0; j < failed_block_num; j++)
        {
          std::string node_ip_port = failed_datanodeip[j] + ":" + std::to_string(failed_datanodeport[j]);
          int t_rack_id = m_datanode2rack[node_ip_port];
          if (t_rack_id != m_self_rack_id)
          {
            size_t t_value_length = block_size;
            std::string t_key = failed_blockkeys[j];
            std::string temp_value = generate_string(t_value_length);
            TransferToNetworkCore(t_key.c_str(), temp_value.c_str(), t_value_length, false, idx);
          }
        }
        gettimeofday(&end_time, NULL);
        cross_rack_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
      }
      catch (const std::exception &e)
      {
        std::cerr << e.what() << '\n';
      }
      // for check
      m_merge_step_processing[0] = false;
      cv.notify_all();
      if (IF_DEBUG)
      {
        std::cout << "\033[1;32m[Main Proxy" << m_self_rack_id << "] finish repair " << failed_block_num << " blocks! " << "\033[0m" << std::endl;
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    return grpc::Status::OK;
  }

  grpc::Status ProxyImpl::helpRepair(
      grpc::ServerContext *context,
      const proxy_proto::helpRepairPlan *help_repair_plan,
      proxy_proto::RepairReply *response)
  {
    bool if_partial_decoding = help_repair_plan->if_partial_decoding();
    std::string proxy_ip = help_repair_plan->mainproxyip();
    int proxy_port = help_repair_plan->mainproxyport();
    int k = help_repair_plan->k();
    int m_g = help_repair_plan->m_g();
    int x_l = help_repair_plan->x_l();
    int seri_num = help_repair_plan->seri_num();
    int block_size = help_repair_plan->block_size();
    ECProject::EncodeType encode_type = (ECProject::EncodeType)help_repair_plan->encodetype();
    bool rv_or_ch__isglobal = help_repair_plan->rv_or_ch__isglobal();
    // int partial_block_num = help_repair_plan->failed_num();
    std::vector<std::string> datanode_ip;
    std::vector<int> datanode_port;
    std::vector<std::string> blockkeys;
    std::vector<int> blockids;
    for (int i = 0; i < help_repair_plan->blockkeys_size(); i++)
    {
      datanode_ip.push_back(help_repair_plan->datanodeip(i));
      datanode_port.push_back(help_repair_plan->datanodeport(i));
      blockkeys.push_back(help_repair_plan->blockkeys(i));
      blockids.push_back(help_repair_plan->blockids(i));
    }
    // for selected parity blocks to repair
    auto parity_idx_ptr = std::make_shared<std::vector<int>>();
    int partial_block_num = help_repair_plan->parity_blockids_size();
    for (int i = 0; i < partial_block_num; i++)
    {
      parity_idx_ptr->push_back(help_repair_plan->parity_blockids(i) - k);
    }

    bool flag = true;
    if (int(blockkeys.size()) <= partial_block_num)
    {
      flag = false;
    }
    if_partial_decoding = (if_partial_decoding && flag);

    // get data from the datanode
    auto myLock_ptr = std::make_shared<std::mutex>();
    auto blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
    auto blocks_idx_ptr = std::make_shared<std::vector<int>>();
    auto getFromNode = [this, block_size, blocks_ptr, blocks_idx_ptr, myLock_ptr](int block_idx, std::string block_key, std::string node_ip, int node_port) mutable
    {
      std::vector<char> temp(block_size);
      bool ret = GetFromDatanode(block_key.c_str(), block_key.size(), temp.data(), block_size, node_ip.c_str(), node_port, block_idx + 2);

      if (!ret)
      {
        std::cout << "getFromNode !ret" << std::endl;
        return;
      }
      myLock_ptr->lock();
      blocks_ptr->push_back(temp);
      blocks_idx_ptr->push_back(block_idx);
      myLock_ptr->unlock();
    };
    if (IF_DEBUG)
    {
      std::cout << "[Helper Proxy" << m_self_rack_id << "] Ready to read blocks from data node!" << std::endl;
    }
    int block_num = int(blockkeys.size());
    try
    {
      int cross_rack_num = 0;
      std::vector<std::thread> read_threads;
      for (int j = 0; j < block_num; j++)
      {
        std::string node_ip_port = datanode_ip[j] + ":" + std::to_string(datanode_port[j]);
        int t_rack_id = m_datanode2rack[node_ip_port];
        if (t_rack_id != m_self_rack_id)
        {
          cross_rack_num++;
        }
        read_threads.push_back(std::thread(getFromNode, blockids[j], blockkeys[j], datanode_ip[j], datanode_port[j]));
      }
      for (int j = 0; j < block_num; j++)
      {
        read_threads[j].join();
      }
      // simulate cross-rack transfer
      if (int(m_networkcore.size()) == 2 && cross_rack_num > 0 && if_partial_decoding)
      {
        std::string t_key = blockkeys[0];
        size_t t_value_length = block_size * cross_rack_num;
        std::string temp_value = generate_string(t_value_length);
        TransferToNetworkCore(t_key.c_str(), temp_value.c_str(), t_value_length, false, 0);
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << e.what() << '\n';
    }

    if (block_num != int(blocks_ptr->size()))
    {
      std::cout << "[Help] can't get enough blocks!" << std::endl;
    }

    std::vector<char *> v_data(block_num);
    std::vector<char *> v_coding(partial_block_num);
    char **data = (char **)v_data.data();
    char **coding = (char **)v_coding.data();
    std::vector<std::vector<char>> v_data_area(block_num, std::vector<char>(block_size));
    std::vector<std::vector<char>> v_coding_area(partial_block_num, std::vector<char>(block_size));
    for (int j = 0; j < block_num; j++)
    {
      data[j] = v_data_area[j].data();
    }
    for (int j = 0; j < partial_block_num; j++)
    {
      coding[j] = v_coding_area[j].data();
    }
    for (int j = 0; j < block_num; j++)
    {
      memcpy(data[j], (*blocks_ptr)[j].data(), block_size);
    }

    // encode partial blocks
    if (if_partial_decoding) // partial encoding
    {
      if (IF_DEBUG)
      {
        std::cout << "[Helper Proxy" << m_self_rack_id << "] partial encoding!" << std::endl;
        for (auto it = blocks_idx_ptr->begin(); it != blocks_idx_ptr->end(); it++)
        {
          std::cout << (*it) << " ";
        }
        std::cout << std::endl;
      }

      double t_time = 0.0;
      std::string et = "";
      gettimeofday(&start_time, NULL);
      if (encode_type == ECProject::HPC)
      {
        encode_partial_blocks_for_repair_HPC(x_l, seri_num, rv_or_ch__isglobal, k, m_g, data, coding, block_size, blocks_idx_ptr, block_num, parity_idx_ptr);
        et = "(HPC)";
      }
      else if (encode_type == ECProject::MLEC)
      {
        encode_partial_blocks_for_repair_PC(k, m_g, data, coding, block_size, blocks_idx_ptr, block_num, parity_idx_ptr);
        et = "(MLEC)";
      }
      else if (encode_type == ECProject::Azure_LRC)
      {
        encode_partial_blocks_for_repair_LRC(k, m_g, x_l, data, coding, block_size, blocks_idx_ptr, block_num, parity_idx_ptr);
        et = "(Azure_LRC)";
      }
      else
      {
        encode_partial_blocks_for_repair_RS(k, m_g, data, coding, block_size, blocks_idx_ptr, block_num, parity_idx_ptr);
        et = "(RS)";
      }
      gettimeofday(&end_time, NULL);
      t_time = end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
      std::cout << et << "Help Decode Time: " << t_time << " (block_size=" << block_size / (1024 * 1024) << "MB)." << std::endl;
      response->set_time(t_time);
    }

    // send to main proxy
    asio::error_code error;
    asio::io_context io_context;
    asio::ip::tcp::socket socket(io_context);
    asio::ip::tcp::resolver resolver(io_context);
    asio::error_code con_error;
    if (IF_DEBUG)
    {
      std::cout << "\033[1;36m[Helper Proxy" << m_self_rack_id << "] Try to connect main proxy port " << proxy_port << "\033[0m" << std::endl;
    }
    asio::connect(socket, resolver.resolve({proxy_ip, std::to_string(proxy_port)}), con_error);
    if (!con_error && IF_DEBUG)
    {
      std::cout << "\033[1;36m[Helper Proxy" << m_self_rack_id << "] Connect to " << proxy_ip << ":" << proxy_port << " success!" << std::endl;
    }

    int value_size = 0;

    std::vector<unsigned char> int_buf_self_rack_id = ECProject::int_to_bytes(m_self_rack_id);
    asio::write(socket, asio::buffer(int_buf_self_rack_id, int_buf_self_rack_id.size()), error);
    if (!if_partial_decoding)
    {
      std::vector<unsigned char> t_flag = ECProject::int_to_bytes(0);
      asio::write(socket, asio::buffer(t_flag, t_flag.size()), error);
      std::vector<unsigned char> int_buf_num_of_blocks = ECProject::int_to_bytes(block_num);
      asio::write(socket, asio::buffer(int_buf_num_of_blocks, int_buf_num_of_blocks.size()), error);
      int j = 0;
      for (auto it = blocks_idx_ptr->begin(); it != blocks_idx_ptr->end(); it++, j++)
      {
        // send index and value
        int block_idx = *it;
        std::vector<unsigned char> byte_block_idx = ECProject::int_to_bytes(block_idx);
        asio::write(socket, asio::buffer(byte_block_idx, byte_block_idx.size()), error);
        asio::write(socket, asio::buffer(data[j], block_size), error);
        value_size += block_size;
      }
    }
    else
    {
      std::vector<unsigned char> t_flag = ECProject::int_to_bytes(1);
      asio::write(socket, asio::buffer(t_flag, t_flag.size()), error);
      for (int j = 0; j < partial_block_num; j++)
      {
        asio::write(socket, asio::buffer(coding[j], block_size), error);
        value_size += block_size;
      }
    }
    asio::error_code ignore_ec;
    socket.shutdown(asio::ip::tcp::socket::shutdown_send, ignore_ec);
    socket.close(ignore_ec);
    if (IF_DEBUG)
    {
      std::cout << "[Helper Proxy" << m_self_rack_id << "] Send value to proxy" << proxy_port << "! With length of " << value_size << std::endl;
    }

    return grpc::Status::OK;
  }

  // block relocation
  // get -> set -> delete
  grpc::Status ProxyImpl::blockReloc(
      grpc::ServerContext *context,
      const proxy_proto::blockRelocPlan *reloc_plan,
      proxy_proto::blockRelocReply *response)
  {
    std::vector<std::string> blocks_id;
    std::vector<std::string> src_node_ip;
    std::vector<int> src_node_port;
    std::vector<std::string> des_node_ip;
    std::vector<int> des_node_port;
    int block_size = reloc_plan->block_size();
    for (int i = 0; i < reloc_plan->blocktomove_size(); i++)
    {
      blocks_id.push_back(reloc_plan->blocktomove(i));
      src_node_ip.push_back(reloc_plan->fromdatanodeip(i));
      src_node_port.push_back(reloc_plan->fromdatanodeport(i));
      des_node_ip.push_back(reloc_plan->todatanodeip(i));
      des_node_port.push_back(reloc_plan->todatanodeport(i));
    }
    auto relocate_blocks = [this, blocks_id, block_size, src_node_ip, src_node_port, des_node_ip, des_node_port]() mutable
    {
      auto relocate_single_block = [this](int j, std::string block_key, int block_size, std::string src_node_ip, int src_node_port, std::string des_node_ip, int des_node_port)
      {
        bool ret = BlockRelocation(block_key.c_str(), block_size, src_node_ip.c_str(), src_node_port, des_node_ip.c_str(), des_node_port);
        if (!ret)
        {
          std::cout << "[Block Relocation] Relocate " << block_key << " Failed!" << std::endl;
        }
        std::string src_ip_port = src_node_ip + ":" + std::to_string(src_node_port);
        bool ret3 = DelInDatanode(block_key, src_ip_port);
        if (!ret3)
        {
          std::cout << "[Block Relocation] Delete in the src node failed : " << block_key << std::endl;
        }
      };
      try
      {
        for (int j = 0; j < int(blocks_id.size()); j++)
        {
          relocate_single_block(j, blocks_id[j], block_size, src_node_ip[j], src_node_port[j], des_node_ip[j], des_node_port[j]);
        }

        m_merge_step_processing[2] = false;
        cv.notify_all();
      }
      catch (const std::exception &e)
      {
        std::cout << "exception" << std::endl;
        std::cerr << e.what() << '\n';
      }
    };
    try
    {
      m_mutex.lock();
      m_merge_step_processing[2] = true;
      m_mutex.unlock();
      std::thread my_thread(relocate_blocks);
      my_thread.detach();
    }
    catch (std::exception &e)
    {
      std::cout << "exception" << std::endl;
      std::cout << e.what() << std::endl;
    }
    return grpc::Status::OK;
  }

  // check
  grpc::Status ProxyImpl::checkStep(
      grpc::ServerContext *context,
      const proxy_proto::AskIfSuccess *step,
      proxy_proto::RepIfSuccess *response)
  {
    std::unique_lock<std::mutex> lck(m_mutex);
    int idx = step->step();
    if (IF_DEBUG)
    {
      std::cout << "\033[1;34m[Main Proxy" << m_self_rack_id << "] Step" << idx << ":" << m_merge_step_processing[idx] << "\033[0m\n";
    }
    while (m_merge_step_processing[idx])
    {
      cv.wait(lck);
    }
    response->set_ifsuccess(true);
    response->set_cross_rack_time(cross_rack_time);
    response->set_inner_rack_time(inner_rack_time);
    response->set_encoding_time(encoding_time);
    response->set_decoding_time(decoding_time);
    cross_rack_time = 0;
    inner_rack_time = 0;
    encoding_time = 0;
    decoding_time = 0;
    return grpc::Status::OK;
  }

} // namespace ECProject
