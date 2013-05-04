#pragma once

#include <boost/exception/exception.hpp>
#include <boost/exception/error_info.hpp>
#include <boost/exception/diagnostic_information.hpp>
#include <boost/throw_exception.hpp>

typedef boost::error_info<struct tag_rpc_code, int> errinfo_rpc_code;
typedef boost::error_info<struct tag_message, std::string> errinfo_message;

struct common_exception
    : virtual boost::exception
    , virtual std::exception
{
};

namespace rpc_error {
    static const int UNKNOWN_ERROR = 0xDEAD;

    static const int STORE_ALREADY_EXISTS = 1;
    static const int INVALID_STORE = 2;
    static const int STORE_NOT_FOUND = 3;
    static const int OPERATION_NOT_SUPPORTED = 4;
}

#define RPC_REPORT_EXCEPTIONS(reply) \
    catch (boost::exception const& e) \
    { if (int const * c = boost::get_error_info<errinfo_rpc_code>(e)) \
        reply.Error(*c, boost::diagnostic_information(e)); \
      else reply.Error(::rpc_error::UNKNOWN_ERROR, boost::diagnostic_information(e)); \
      return; } \
    catch (std::exception const& e) \
    { reply.Error(::rpc_error::UNKNOWN_ERROR, e.what()); return; }
