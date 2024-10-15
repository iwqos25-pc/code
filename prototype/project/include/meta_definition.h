#ifndef META_DEFINITION
#define META_DEFINITION
#include "devcommon.h"
namespace ECProject
{
  enum OpperateType
  {
    SET,
    GET,
    DEL,
    REPAIR,
    MERGE
  };
  enum EncodeType
  {
    HPC,
    MLEC,
    Azure_LRC,
    RS
  };
  enum FailureType
  {
    Single_Block,
    Rand_Multi_Blocks,
    Single_Rack
  };
  enum SingleStripePlacementType
  {
    Random,
    Flat,
    Optimal
  };
  enum MultiStripesPlacementType
  {
    // for HPC
    Vertical,
    Horizontal,
    // for LRC
    Ran,
    DIS,
    AGG,
    OPT
  };

  typedef struct Block
  {
    int block_id;          // to denote the order of data blocks in a stripe
    std::string block_key; // to distinct block, globally unique
    char block_type;
    int block_size;
    int map2group, map2row, map2col;
    int map2stripe, map2rack, map2node;
    std::string map2key;
    Block(int block_id, const std::string block_key, char block_type, int block_size, int map2group,
          int map2stripe, int map2rack, int map2node, const std::string map2key)
        : block_id(block_id), block_key(block_key), block_type(block_type), block_size(block_size),
          map2group(map2group), map2stripe(map2stripe), map2rack(map2rack),
          map2node(map2node), map2key(map2key)
    {
      map2row = 0;
      map2col = 0;
    }
    Block(int block_id, const std::string block_key, char block_type, int block_size, int map2row, int map2col,
          int map2stripe, int map2rack, int map2node, const std::string map2key)
        : block_id(block_id), block_key(block_key), block_type(block_type), block_size(block_size),
          map2row(map2row), map2col(map2col), map2stripe(map2stripe),
          map2rack(map2rack), map2node(map2node), map2key(map2key)
    {
      map2group = 0;
    }
    Block()
    {
      map2group = 0;
      map2row = 0;
      map2col = 0;
    }
  } Block;

  typedef struct Rack
  {
    int rack_id;
    std::string proxy_ip;
    int proxy_port;
    std::vector<int> nodes;
    std::vector<Block *> blocks;
    std::unordered_set<int> stripes;
    Rack(int rack_id, const std::string &proxy_ip, int proxy_port) : rack_id(rack_id), proxy_ip(proxy_ip), proxy_port(proxy_port) {}
    Rack() = default;
  } Rack;

  typedef struct Node
  {
    int node_id;
    std::string node_ip;
    int node_port;
    int map2rack;
    std::unordered_map<int, int> stripes; // stripe_id and block_number
    Node(int node_id, const std::string node_ip, int node_port, int map2rack) : node_id(node_id), node_ip(node_ip), node_port(node_port), map2rack(map2rack) {}
    Node() = default;
  } Node;

  typedef struct Stripe
  {
    int k, l, g_m;
    int k1, m1, k2, m2;
    int stripe_id;
    std::vector<std::string> object_keys;
    // std::vector<int> object_sizes;
    std::vector<Block *> blocks;
    std::unordered_set<int> place2racks;
  } Stripe;

  typedef struct ObjectInfo
  {
    std::string object_key;
    int object_size;
    int map2stripe;
  } ObjectInfo;

  typedef struct ECSchema
  {
    ECSchema() = default;
    ECSchema(bool partial_decoding, bool approach, EncodeType encodetype, SingleStripePlacementType s_stripe_placementtype,
             MultiStripesPlacementType m_stripe_placementtype, int k_datablock, int l_localparityblock, int g_m_globalparityblock,
             int x_stripepermergegroup)
        : partial_decoding(partial_decoding), approach(approach), encodetype(encodetype), s_stripe_placementtype(s_stripe_placementtype),
          m_stripe_placementtype(m_stripe_placementtype), k_datablock(k_datablock), l_localparityblock(l_localparityblock),
          g_m_globalparityblock(g_m_globalparityblock), x_stripepermergegroup(x_stripepermergegroup)
    {
      b_datapergroup = std::ceil(((double)k_datablock / (double)l_localparityblock));
      k1_col_datablock = 0;
      m1_col_parityblock = 0;
      k2_row_datablock = 0;
      m2_row_parityblock = 0;
    }
    ECSchema(bool partial_decoding, bool approach, EncodeType encodetype, SingleStripePlacementType s_stripe_placementtype,
             MultiStripesPlacementType m_stripe_placementtype, int k1_col_datablock, int m1_col_parityblock,
             int k2_row_datablock, int m2_row_parityblock, int x_stripepermergegroup)
        : partial_decoding(partial_decoding), approach(approach), encodetype(encodetype), s_stripe_placementtype(s_stripe_placementtype),
          m_stripe_placementtype(m_stripe_placementtype), k1_col_datablock(k1_col_datablock), m1_col_parityblock(m1_col_parityblock),
          k2_row_datablock(k2_row_datablock), m2_row_parityblock(m2_row_parityblock), x_stripepermergegroup(x_stripepermergegroup)
    {
      k_datablock = k1_col_datablock * k2_row_datablock;
      l_localparityblock = 0;
      g_m_globalparityblock = 0;
      b_datapergroup = 0;
    }
    bool partial_decoding;
    bool approach;
    EncodeType encodetype;
    SingleStripePlacementType s_stripe_placementtype;
    MultiStripesPlacementType m_stripe_placementtype;
    int k_datablock;
    int l_localparityblock;
    int g_m_globalparityblock;
    int b_datapergroup;
    int k1_col_datablock;
    int m1_col_parityblock;
    int k2_row_datablock;
    int m2_row_parityblock;
    int x_stripepermergegroup; // the product of xi
  } ECSchema;
} // namespace ECProject

#endif // META_DEFINITION