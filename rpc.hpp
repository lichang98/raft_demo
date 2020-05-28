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
#include <unordered_map>
#include <tuple>
#include <vector>
#include <glog/logging.h>
#include <glog/log_severity.h>
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
        const static int32_t REQUEST_VOTE=0;
        const static int32_t APPEND_ENTRIES=1;
        const static int32_t INSTALL_SNAPSHOT=2; 
    };


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
        int32_t term; // leader's term
        int32_t leader_id; // client save it, then can redirect request to leader
        int32_t prev_log_index; // index of log entry immediately preceding new ones
        int32_t prev_log_term; // term of prev_log_index entry
        void* entries; // log entries to store;
        int32_t leader_commit; // leader's commit index

        // values for return
        int32_t current_term; // current term for leader to update itself
        bool succ; // true if follower contained entry matching pre_log_index and prev_log_term
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
        void* data; // raw bytes of the snapshot chunk, starting at offset
        bool done; // true if this is the last chunk

        // values for return
        int32_t current_term; // for leader to update itself
    };
    
   /*
    * the send/recv data struct
    * params: the pointer to different kinds of rpc struct
    * type: the type of rpc, which help convert the params pointer
    */
    struct rpc_data
    {
        void *params;
        int32_t type;

        int32_t src_server_index, dest_server_index;
    };
    static int32_t RECV_BACKLOG_SIZE = 32; // the max size of the server listen backlog
    const static int32_t RECV_BUFFER_SIZE = sizeof(rpc::rpc_data); // the size of each RPC

    sockaddr_in convert_ip_port_to_addr(const char* ip_addr, int32_t port)
    {
        sockaddr_in addr;
        memset(&addr,0,sizeof(sockaddr_in));
        addr.sin_family = AF_INET;
        addr.sin_port = port;
        if(inet_aton(ip_addr,&addr.sin_addr) < 0)
        {
            perror("inet_aton");
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
        }

        void create_socket()
        {
            this->self_socket_fd = socket(AF_INET,SOCK_STREAM,0);
            if(this->self_socket_fd < 0)
            {
                perror("socker");
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
            if(this->self_socket_fd < 0)
            {
                perror("incorrect socket file description!");
                return false;
            }
            sockaddr_in serv_addr;
            memset(&serv_addr, 0,sizeof(sockaddr_in));
            serv_addr.sin_family=AF_INET;
            serv_addr.sin_port=htons(port);
            serv_addr.sin_addr.s_addr=htonl(INADDR_ANY);
            if(bind(this->self_socket_fd, (const sockaddr*)&serv_addr,sizeof(sockaddr_in)) < 0)
            {
                perror("bind");
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
            sockaddr_in server_addr;
            memset(&server_addr,0,sizeof(sockaddr_in));
            server_addr.sin_family=AF_INET;
            server_addr.sin_port = htons(_server_port);
            if(inet_aton(_serv_addr, &server_addr.sin_addr) <0)
            {
                perror("inet_atop");
                exit(1);
            }
            if(connect(this->self_socket_fd,(const sockaddr *)&server_addr,sizeof(sockaddr_in))< 0)
            {
                perror("connect");
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
                return false;
            if(listen(this->self_socket_fd,RECV_BACKLOG_SIZE) < 0)
            {
                perror("listen");
                exit(1);
            }
            RECV_BACKLOG_SIZE--;
            sockaddr_in client_addr;
            socklen_t addr_len=sizeof(sockaddr_in);
            int32_t client_sock_fd = accept(this->self_socket_fd,(sockaddr*)&client_addr,&addr_len);

            accepted_socket_fds.push_back(client_sock_fd);

            // if(getpeername(client_sock_fd,(sockaddr*)&client_addr,&addr_len) < 0)
            // {
            //     perror("getpeername");
            //     exit(1);
            // }
            // char *ip_addr = inet_ntoa(client_addr.sin_addr);
            // printf("accept ip=%s, port=%u\n",ip_addr, ntohs(client_addr.sin_port));

            if(client_sock_fd <0)
            {
                perror("accept");
                exit(1);
            }

            return true;
        }

        /*
        * send message to the target socket
        * Notice that, this function only used by the server who connected from client
        * notice that the sended message struct need have a type attribute, the possible values are in rpc_type
        */
        ssize_t send_msg(int32_t target_sock_fd, const void* msg, size_t len)
        {
            int32_t real_send_len=0;
            if((real_send_len = send(target_sock_fd, (const void*)msg, len,0) )<0)
            {
                perror("send");
                exit(1);
            }
            return real_send_len;
        }

        /*
        * client use this message to send server that have build connection
        */
        ssize_t client_send_msg(void *msg, size_t msg_len)
        {
            ssize_t real_send = write(this->self_socket_fd, msg,msg_len);
            if(real_send < 0)
            {
                perror("write");
                exit(1);
            }
            return real_send;
        }

        /*
        * receive data from the socket created at localhost, it will block until data arrive
        * Notice that the 
        */
        std::tuple<int32_t, void*> recv_data(const int32_t timeout_millisec=3)
        {
            // char *recv_buf = new char[RECV_BUFFER_SIZE];
            rpc_data* data = new rpc_data();
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
                return std::make_tuple(-1,nullptr);
            }
            else
            {
                ssize_t real_recv = read(this->self_socket_fd,data,RECV_BUFFER_SIZE);
                return std::make_tuple(real_recv, (void*)data);
            }
        }

        /*
        *   this function used for server to receive data
        * 
        */
        rpc::rpc_data *server_recv_data(const int32_t timeout_millisec=3)
        {
            rpc::rpc_data *data=new rpc::rpc_data();
            fd_set rd;
            timeval tv;
            int32_t flag=0;
            FD_ZERO(&rd);
            FD_SET(0,&rd);
            tv.tv_sec=0;
            tv.tv_usec=timeout_millisec*1000;
            flag = select(1,&rd,nullptr,nullptr,&tv);
            if(flag==0 || flag==-1)
            {
                // timeout
                return nullptr;
            }
            else
            {
                ssize_t real_recv_len = read(this->self_socket_fd,(void*)data,RECV_BUFFER_SIZE);
                if(real_recv_len <0)
                {
                    perror("socket read");
                    exit(1);
                }
                return data;
            }
        }

        void close_socket()
        {
            if(close(this->self_socket_fd) < 0)
            {
                perror("close socket");
                exit(1);
            }
            delete[] server_ip;
            server_ip=nullptr;
            RECV_BACKLOG_SIZE++;
        }

        // std::unordered_map<const sockaddr_in, int32_t> addr_sockfd_map;
        std::vector<int32_t> accepted_socket_fds; // only used when current server as the center
        int32_t self_socket_fd;
        // when current server acts as a client, this variable record the server connects with
        char *server_ip;
        int32_t server_port;
    };

};