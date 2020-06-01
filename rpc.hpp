#pragma once
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <string.h>
#include <utility>
#include <chrono>
#include <thread>
#include <unordered_map>
#include <tuple>
#include <vector>
#include <glog/logging.h>
#include <glog/log_severity.h>
#include "data_type.hpp"
/*
* in raft, each server can be candidate, follower and leader
* follower only receive RPCs except when not sense leader after timeout and become candidate
* there are some basic rules for servers in system:
* for all servers:
*      if commitIndex > lastApplied, increment lastApplied, apply log[lastApplied] to state machine
*      if RPC request and response contains term T > it self's current Term, set currentTerm=T,
*           and convert to follower
* for followers:
*       respond to RPCs from cndidates and leader
*       if election timeout elapses without receiveing AppendEntries RPC from current leader
*           or granting vote to candidate, convert to candidate and request for vote
* for candidates:
*       on conversion to candidates, start election:
*           increment currentTerm
*           vote for itself
*           reset election timer
*           send requestVote RPCs to all other servers
*       if votes received from majority of servers, become leader
*       if AppendEntries RPC received from new leader, convert to follower
*       if election timeout elapses, start new election
* for leaders:
*       upon election, send initial empty AppendEntries RPCs as heartbeat to each server,
*       repeat during idle periods to prevent election timeout
*       if command received from client, append entries to local log, respond afer applied to state machine
*       if last log index \geq nextIndex for a follower, send AppendEntries RPC with log entries 
*       starting ar nextIndex:
*           if succ, update nextIndex and matchIndex for followers
*           if fails because of log inconsistency, decrement nextIndex and retry
*       if there exists an N such that N > commitIndex, a majority of matchIndex[i] \geq N,
*       and log[N].term == currentTerm, set commitIndex = N
*/
namespace rpc
{
    namespace rpc_type
    {
        const static int32_t CONN=0;
        const static int32_t REQUEST_VOTE=1;
        const static int32_t APPEND_ENTRIES=2;
        const static int32_t INSTALL_SNAPSHOT=3; 
        const static int32_t CLIENT_REQUEST=4;
    };

    static int32_t RECV_BACKLOG_SIZE = 32; // the max size of the server listen backlog
    const static int32_t RPC_MAX_DATA_BRING = 8; // the maximum number of data the RPC can bring
    // this rpc invoked by candidates to gather votes
    struct rpc_requestvote
    {
        int32_t term; // candidate's term
        int32_t candidate_id; // candidate requesting vote
        int32_t last_log_index; // index of candidate's last log entry
        int32_t last_log_term; // term of candidate's last log entry

        // values for return
        // for the receiver, 
        // if the candidate's log is at least as up-to-date as receiver's, grant vote
        int32_t current_term; // current term, for candidate to update itself
        bool vote_granted; // true means candidate receoved vote

        /*
        * the size after the struct serialized as string
        */
        static int32_t serialize_size()
        {
            return 21;
        }

        /*
        * the data transmission of data need to serialize
        */
        void serialize(char *&serialize_msg)
        {
            serialize_msg = new char[21]; // the total size of elements of struct is 21 bytes(no alignment)
            int32_t *p = (int32_t*)serialize_msg;
            *p = term;++p;
            *p = candidate_id;++p;
            *p = last_log_index;++p;
            *p = last_log_term;++p;
            *p= current_term;++p;
            char* q=(char*)p;
            *q = vote_granted ? '1':'0';
        }
        /*
        * reconstruct data of struct from serialized string message
        */
        void deserialize(char *serialize_msg)
        {
            int32_t* p = (int32_t*)serialize_msg;
            term=*p;++p;
            candidate_id=*p;++p;
            last_log_index=*p;++p;
            last_log_term=*p;++p;
            current_term=*p;++p;
            char* q=(char*)p;
            vote_granted=*q=='1';
        }
    };

    // this rpc invoked by leader to replicate log entries
    // also used as heartbeat
    // for follower server's, after receiving the RPC
    //      reply false if leader's term < current term
    //      reply false if entries does not contain any entry at
    //      prev_log_index whose term matches prev_log_term
    //      if the existing entry on follower conflicts with new one from leader
    //      (same index, different term), delete all and all after
    //      append new entries to it's log
    //      if leader commit > it's own commit index, 
    //      set it's commit index=min(leader_commit, index of last new entry)
    struct rpc_append_entries
    {
        // leader's term
        int32_t term;
        // client save it, then can redirect request to leader
        int32_t leader_id; 
        // index of log entry immediately preceding new ones
        int32_t prev_log_index;
        // term of prev_log_index entry
        int32_t prev_log_term; 
        // log entries to store;
        my_data_type::log_entry entries[RPC_MAX_DATA_BRING];
        int32_t real_bring;
        // leader's commit index
        int32_t leader_commit;

        // values for return
        int32_t current_term; // current term for leader to update itself
        bool succ; // true if follower contained entry matching pre_log_index and prev_log_term

        static int32_t serialize_size()
        {
            return 29+RPC_MAX_DATA_BRING*16;
        }

        void serialize(char*& msg)
        {
            int32_t size=28+1+RPC_MAX_DATA_BRING*16;
            msg = new char[size];
            int32_t *p = (int32_t*)msg;
            *p = term;++p;
            *p = leader_id;++p;
            *p=prev_log_index;++p;
            *p=prev_log_term;++p;
            for(int32_t i=0;i<RPC_MAX_DATA_BRING;++i)
            {
                *p = entries[i].data.k;++p;
                *p = entries[i].data.v;++p;
                *p = entries[i].index;++p;
                *p = entries[i].term;++p;
            }
            *p = real_bring;++p;
            *p = leader_commit;++p;
            *p = current_term;++p;
            char* q=(char*)p;
            *q = succ ? '1':'0';
        }

        void deserialize(const char* msg)
        {
            int32_t *p = (int32_t*)msg;
            term=*p;++p;
            leader_id=*p; ++p;
            prev_log_index = *p; ++p;
            prev_log_term = *p; ++p;
            for(int32_t i=0;i<RPC_MAX_DATA_BRING;++i)
            {
                entries[i].data.k = *p ;++p;
                entries[i].data.v =*p; ++p;
                entries[i].index = *p; ++p;
                entries[i].term = *p; ++p;
            }
            real_bring = *p;++p;
            leader_commit=*p; ++p;
            current_term=*p; ++p;
            char* q = (char*)p;
            succ = *q == '1';
        }
    };
    
    // this rpc invoked by leader to send chunks of snapshot to a follower
    // for receiver, if leader's term < it's own current term, reply immediately
    // create new snapshot file if first chunk (offset =0)
    // write data into snap shot file at given offset
    // reply and wait for more data chunks if done is false
    // save snapshot file, discard any existing or partial snapshot with with smaller index
    // if existing log entry has same index and term as snapshot's last included entry, retain
    // log entries following it and reply
    // discard the entire log
    // reset state machine using snapshot contents, and load snapshot's cluster configuration
    struct rpc_install_snapshot_rpc
    {
        int32_t term; // leader's term
        int32_t leader_id; // follower save this value and can redirect requests to leader
        int32_t last_include_index; // the snapshot replaces all entries up through and including this index
        int32_t last_include_term; // the term of last include index
        int32_t offset; // byte offset where chunk is positioned in the snapshot file
        my_data_type::log_entry data[RPC_MAX_DATA_BRING]; // snapshot chunk, starting at offset
        int32_t real_bring;
        // values for return
        int32_t current_term; // for leader to update itself
        bool done; // true if this is the last chunk

        static int32_t serialize_size()
        {
            return 29+16*RPC_MAX_DATA_BRING;
        }

        void serialize(char*& msg)
        {
            int32_t size=28+1+16*RPC_MAX_DATA_BRING;
            msg = new char[size];
            int32_t* p = (int32_t*)msg;
            *p = term; ++p;
            *p = leader_id; ++p;
            *p = last_include_index; ++p;
            *p = last_include_term; ++p;
            *p = offset; ++p;
            for(int32_t i=0;i<RPC_MAX_DATA_BRING;++i)
            {
                *p = data[i].data.k;++p;
                *p = data[i].data.v;++p;
                *p = data[i].index;++p;
                *p = data[i].term;++p;
            }
            *p = real_bring; ++p;
            *p = current_term; ++p;
            char* q = (char*)p;
            *q = done ? '1':'0';
        }

        void deserialize(const char* msg)
        {
            int32_t *p = (int32_t*)msg;
            term= *p; ++p;
            leader_id =*p; ++p;
            last_include_index = *p; ++p;
            last_include_term = *p; ++p;
            offset = *p; ++p;
            for(int32_t i=0;i<RPC_MAX_DATA_BRING;++i)
            {
                data[i].data.k= *p; ++p;
                data[i].data.v = *p; ++p;
                data[i].index =*p; ++p;
                data[i].term= *p; ++p;
            }
            real_bring= *p; ++p;
            current_term= *p; ++p;
            char* q=(char*)p;
            done = *q=='1';
        }
    };

    struct client_request
    {
        // the command that client request system to process
        // in this project, int this project, client send one KV pair each time
        my_data_type::log_entry data[RPC_MAX_DATA_BRING]; 
        int32_t real_bring;

        static int32_t serialize_size()
        {
            return 4+16*RPC_MAX_DATA_BRING;
        }

        void serialize(char*& msg)
        {
            int32_t size=16*RPC_MAX_DATA_BRING+4;
            msg = new char[size];
            int32_t *p = (int32_t*)msg;
            for(int32_t i=0;i<RPC_MAX_DATA_BRING;++i)
            {
                *p = data[i].data.k; ++p;
                *p = data[i].data.v; ++p;
                *p = data[i].index; ++p;
                *p = data[i].term; ++p;
            }
            *p = real_bring;
        }

        void deserialize(const char* msg)
        {
            int32_t *p = (int32_t*)msg;
            for(int32_t i=0;i<RPC_MAX_DATA_BRING;++i)
            {
                data[i].data.k =*p; ++p;
                data[i].data.v= *p; ++p;
                data[i].index = *p; ++p;
                data[i].term= *p; ++p;
            }
            real_bring = *p;
        }
    };
    
    
   /*
    * the send/recv data struct
    * params: the pointer to different kinds of rpc struct
    * type: the type of rpc, which help convert the params pointer
    */
    struct rpc_data
    {
        // void* params;
        union params_union
        {
            rpc::rpc_append_entries apd;
            rpc::client_request clt;
            rpc::rpc_install_snapshot_rpc inst;
            rpc::rpc_requestvote req_vote;

            params_union(){memset(this,0,sizeof(params_union));}
        } params;

        int32_t type;

        int32_t src_server_index, dest_server_index;
        bool is_request; // true if is a request, otherwise it is a response

        void deserialize(const char* msg)
        {
            int32_t *p = (int32_t*)msg;
            type = *p; ++p;
            src_server_index = *p; ++p;
            dest_server_index =*p; ++p;
            char* q=(char*)p;
            is_request = *q=='1'; ++q;
            if(type == rpc::rpc_type::APPEND_ENTRIES)
            {
                params.apd.deserialize(q);
            }
            else if(type == rpc::rpc_type::CLIENT_REQUEST)
            {
                params.clt.deserialize(q);
            }
            else if(type == rpc::rpc_type::INSTALL_SNAPSHOT)
            {
                params.inst.deserialize(q);
            }
            else if(type == rpc::rpc_type::REQUEST_VOTE)
            {
                params.req_vote.deserialize(q);
            }
        }

        static int32_t serialize_size()
        {
            int32_t param_sizes[]={rpc_append_entries::serialize_size(),rpc_requestvote::serialize_size(),\
                    client_request::serialize_size(), rpc_install_snapshot_rpc::serialize_size()};
            int32_t max_param_size=0;
            for(int32_t i=0;i<4;++i)
                max_param_size=std::max(max_param_size, param_sizes[i]);
            return max_param_size+13;  
        }

        void serialize(char*& msg)
        {
            int32_t param_sizes[]={rpc_append_entries::serialize_size(),rpc_requestvote::serialize_size(),\
                    client_request::serialize_size(), rpc_install_snapshot_rpc::serialize_size()};
            int32_t max_param_size=0;
            for(int32_t i=0;i<4;++i)
                max_param_size=std::max(max_param_size, param_sizes[i]);
            int32_t len = max_param_size+13;
            msg = new char[len];
            int32_t *p = (int32_t*)msg;
            *p = type; ++p;
            *p = src_server_index; ++p;
            *p = dest_server_index; ++p;
            char* q = (char*)p;
            *q = is_request ? '1':'0';++q;
            if(type == rpc_type::APPEND_ENTRIES)
            {
                char *apd=nullptr;
                params.apd.serialize(apd);
                memcpy(q,apd,params.apd.serialize_size());
            }
            else if(type == rpc_type::CLIENT_REQUEST)
            {
                char* clit = nullptr;
                params.clt.serialize(clit);
                memcpy(q,clit,params.clt.serialize_size());
            }
            else if(type == rpc::rpc_type::INSTALL_SNAPSHOT)
            {
                char* inst = nullptr;
                params.inst.serialize(inst);
                memcpy(q,inst,params.inst.serialize_size());
            }
            else if(type == rpc::rpc_type::REQUEST_VOTE)
            {
                char* req_vote = nullptr;
                params.req_vote.serialize(req_vote);
                memcpy(q,req_vote,params.req_vote.serialize_size());
            }
        }
    };

    sockaddr_in convert_ip_port_to_addr(const char* ip_addr, int32_t port)
    {
        sockaddr_in addr;
        memset(&addr,0,sizeof(sockaddr_in));
        addr.sin_family = AF_INET;
        addr.sin_port = port;
        if(inet_aton(ip_addr,&addr.sin_addr) < 0)
        {
            LOG(ERROR) << "inet_aton conversion failed";
            exit(1);
        }
        return addr;
    }

    class RPCManager
    {
    public:
        RPCManager()
        {
            server_ip = nullptr;
            server_port=-1;
            round_robin=0;
        }

        void create_socket()
        {
            this->self_socket_fd = socket(AF_INET,SOCK_STREAM,0);
            if(this->self_socket_fd < 0)
            {
                LOG(ERROR) << "socket create failed";
                exit(1);
            }
        }

        /*
        * bind socket on a port of localhost machine
        * it is typically for server
        * params:
        *    port: the port to bind with on the localhost machine
        */
        bool bind_socket_port(int32_t port)
        {
            LOG(INFO) << "bind socket at port " << port;
            if(this->self_socket_fd < 0)
            {
                LOG(ERROR) << "incorrect socket file description!";
                return false;
            }
            sockaddr_in serv_addr;
            memset(&serv_addr, 0,sizeof(sockaddr_in));
            serv_addr.sin_family=AF_INET;
            serv_addr.sin_port=htons(port);
            serv_addr.sin_addr.s_addr=htonl(INADDR_ANY);
            if(bind(this->self_socket_fd, (const sockaddr*)&serv_addr,sizeof(sockaddr_in)) < 0)
            {
                LOG(ERROR) << "bind failed, address port already be binded!!";
                exit(1);
            }
            return true;
        }

        /*
        * create connection with server
        * params:
        *   server_addr: the net address of the target server, int the format "127.0.0.1"
        *   server_port: the target port of the connection
        */
        void create_connection(char* _serv_addr, int32_t _server_port)
        {
            LOG(INFO) << "create connection to ip=" << _serv_addr << ", port=" << _server_port;
            sockaddr_in server_addr;
            memset(&server_addr,0,sizeof(sockaddr_in));
            server_addr.sin_family=AF_INET;
            server_addr.sin_port = htons(_server_port);
            if(inet_aton(_serv_addr, &server_addr.sin_addr) <0)
            {
                LOG(ERROR) << "inet_aton filed!";
                exit(1);
            }
            if(connect(this->self_socket_fd,(const sockaddr *)&server_addr,sizeof(sockaddr_in))< 0)
            {
                LOG(ERROR) << "connection filed, the server address may be incorrect, with server ip=" << _serv_addr\
                        << ", server port=" << _server_port;
                exit(1);
            }
            // client record the countpart server's ip and port
            server_ip = new char[strlen(_serv_addr)+1];
            strncpy(server_ip,_serv_addr,sizeof(_serv_addr));
            server_ip[strlen(_serv_addr)]='\0';
            this->server_port=_server_port;
        }

        /*
        * server call this function to start listen requests
        * return false when the back log size is 0
        */
        bool listen_and_accept(int32_t timeout_milliseconds=10)
        {
            LOG(INFO) << "server listen for connection";
            if(RECV_BACKLOG_SIZE <=0)
                return false;
            fd_set rd;
            timeval tv;
            FD_ZERO(&rd);
            FD_SET(this->self_socket_fd,&rd);
            tv.tv_sec=0;
            tv.tv_usec=timeout_milliseconds*1000;
            int32_t flag =select(this->self_socket_fd+1,&rd,nullptr,nullptr,&tv);
            if(flag == 0 || flag ==-1)
            {
                LOG(INFO) << "server listen timeout";
                return false;
            }
            if(listen(this->self_socket_fd,RECV_BACKLOG_SIZE) < 0)
            {
                LOG(ERROR) << "server listen failed!";
                exit(1);
            }
            RECV_BACKLOG_SIZE--;
            sockaddr_in client_addr;
            socklen_t addr_len=sizeof(sockaddr_in);
            int32_t client_sock_fd = accept(this->self_socket_fd,(sockaddr*)&client_addr,&addr_len);
            accepted_socket_fds.push_back(client_sock_fd);
            if(client_sock_fd <0)
            {
                LOG(ERROR) << "server accept connection failed!";
                exit(1);
            }
            LOG(INFO) << "server listen and accepted a client";
            return true;
        }

        /*
        * send message to the target socket
        * Notice that, this function only used by the server
        * because server send data onto accepted socket
        */
        ssize_t send_msg(int32_t target_sock_fd, const void* msg, size_t len)
        {
            int32_t real_send_len=0;
            if((real_send_len = send(target_sock_fd, msg, len,0) )<0)
            {
                LOG(ERROR) << "server send message failed!";
                exit(1);
            }
            return real_send_len;
        }

        /*
        * client use this message to send server that have build connection
        * different server's behaviour, client just need to write data on self's socket, and server can receive
        */
        ssize_t client_send_msg(void *msg, size_t msg_len)
        {
            ssize_t real_send = write(this->self_socket_fd, msg,msg_len);
            LOG(INFO) << "client sent data with size=" << msg_len << ", real send=" << real_send;
            if(real_send < 0)
            {
                LOG(ERROR) << "client send message failed!";
                exit(1);
            }
            return real_send;
        }

        /*
        * receive data from the socket created at localhost, it will block until data arrive
        * Notice that the function is special for client
        * client read self's socket to receive data from server
        */
        std::tuple<int32_t, char*> recv_data(size_t read_size,const int32_t timeout_millisec=3)
        {
            LOG(INFO) << "client try receive data";
            char* data = new char[read_size];
            bzero(data,read_size);
            fd_set rd;
            timeval tv;
            FD_ZERO(&rd);
            FD_SET(this->self_socket_fd,&rd);
            tv.tv_usec = timeout_millisec*1000;
            tv.tv_sec=0;
            int32_t ret = select(this->self_socket_fd+1,&rd,nullptr,nullptr,&tv);
            if(ret == 0 || ret==-1)
            {
                // read timeout
                LOG(INFO) << "client try read timeout";
                return std::make_tuple(-1,nullptr);
            }
            else
            {
                ssize_t real_recv = read(this->self_socket_fd,(void*)data,read_size);
                LOG(INFO) << "client try read, size=" << real_recv;
                return std::make_tuple(real_recv, data);
            }
        }

        /*
        *   this function used for server to receive data
        *   different from client, server need to read from accepted client sockets to receive data from clients
        *   
        */
        char *server_recv_data(size_t read_size,const int32_t timeout_millisec=3)
        {
            LOG(INFO) << "server try receive";
            char* data = new char[read_size];
            bzero(data,read_size);

            fd_set rd;
            timeval tv;
            int32_t flag=0;
            FD_ZERO(&rd);
            int32_t max_val=0;
            for(int32_t sock_fd : accepted_socket_fds)
            {
                FD_SET(sock_fd, &rd);
                max_val=std::max(max_val, sock_fd);
            }
            tv.tv_sec=0;
            tv.tv_usec=timeout_millisec*1000;
            flag = select(max_val+1, &rd,nullptr,nullptr,&tv);
            if(flag==0|| flag==-1)
            {
                // timeout
                LOG(INFO) << "server try receive timeout";
                return nullptr;
            }
            else
            {
                int32_t i=round_robin,count=0,size=accepted_socket_fds.size();
                while(count < size)
                {
                    count++;
                    i = (i+1) % size;
                    int32_t sock_fd_ele = accepted_socket_fds[i];
                    if(FD_ISSET(sock_fd_ele, &rd))
                    {
                        ssize_t real_recv_len = read(sock_fd_ele, (void*)data, read_size);
                        LOG(INFO) << "server real recv data size=" << real_recv_len;
                        round_robin=i;
                        if(real_recv_len <0)
                        {
                            LOG(ERROR) << "socket read failed for server";
                            return nullptr;
                        }
                        else
                        {
                            return data;
                        }
                    }
                }
            }
        }

        void close_socket()
        {
            if(close(this->self_socket_fd) < 0)
            {
                LOG(ERROR) << "socket close failed!";
                exit(1);
            }
            delete[] server_ip;
            server_ip=nullptr;
            RECV_BACKLOG_SIZE++;
        }

        // std::unordered_map<const sockaddr_in, int32_t> addr_sockfd_map;
        std::vector<int32_t> accepted_socket_fds; // only used when current server as the center
        int32_t self_socket_fd;
        // the center server will try to receive the data from client sockets created before
        // when multiple accepted client socket has data, in order to read from each socket evenly
        // when server read from a client socket, next epoch, will start from prev pos+1
        int32_t round_robin;
        // when current server acts as a client, this variable record the server connects with
        char *server_ip;
        int32_t server_port;
    };

};