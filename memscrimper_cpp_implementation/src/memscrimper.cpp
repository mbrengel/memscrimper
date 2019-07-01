// Copyright [2019] <Daniel Weber>

#include <cstdlib>

#include <iostream>
#include <stdexcept>

#include <boost/core/null_deleter.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks/text_ostream_backend.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/smart_ptr/make_shared_object.hpp>
#include <boost/smart_ptr/shared_ptr.hpp>

#include "interdedup_compress.h"
#include "interdedup_decompress.h"
#include "request_handler.h"
#include "socket_api.h"
#include "memdump.h"


#ifndef DEBUGMODE
#define DEBUGMODE 0
#endif

typedef boost::log::sinks::synchronous_sink<boost::log::sinks::text_file_backend> FileSink;

template<bool isconsole>
void log_fmt(boost::log::record_view const& rec,
             boost::log::formatting_ostream& stream) {
  namespace log = boost::log;
  namespace ptime = boost::posix_time;

  // add color on terminal
  auto severity = rec[log::trivial::severity];
  if (isconsole && severity) {
    switch (severity.get()) {
    case log::trivial::debug:
        stream << "\033[36m";
        break;
    case log::trivial::info:
        stream << "\033[32m";
        break;
    case log::trivial::warning:
        stream << "\033[33m";
        break;
    case log::trivial::error:
        stream << "\033[31m";
        break;
    default:
        break;
    }
  }

  // extract timestamp + thread id
  auto timestamp = log::extract<boost::posix_time::ptime>("TimeStamp", rec);
  auto tid = log::extract<log::thread_id>("ThreadID", rec);

  // format timestamp
  ptime::time_facet *df = new ptime::time_facet("%Y-%m-%d %H:%M:%S%F");
  stream.imbue(std::locale(std::locale::classic(), df));

  // format actual message
  stream << "[" << timestamp << "|";
  const char *str = log::trivial::to_string(severity.get());
  for (int i = 0; i < 3; i++) {
    stream << static_cast<char>(std::toupper(str[i]));
  }
  stream << "][" << tid << "] ";

  // change color back to normal if necessary
  if (isconsole && severity) {
      stream << "\033[0m";
  }

  // print actual message
  stream << rec[log::expressions::smessage];
}


void init_file_collecting(boost::shared_ptr<FileSink> sink, const uint32_t logsize,
        const uint32_t number_diff_logfiles) {
  // configure logfile rotation
  namespace keywords = boost::log::keywords;
  namespace sinks = boost::log::sinks;
  sink->locked_backend()->set_file_collector(sinks::file::make_collector(
          keywords::target = "logs",  // store logfiles in folder named "logs"
          keywords::max_size = logsize * number_diff_logfiles,
          keywords::max_files = number_diff_logfiles));
}


void init_logging() {
  namespace log = boost::log;
  namespace sinks = log::sinks;
  namespace trivial = log::trivial;
  namespace keywords = boost::log::keywords;

  typedef sinks::synchronous_sink<sinks::text_ostream_backend> text_sink;
  const uint32_t logsize = 1 * 1024 * 1024;
  const uint32_t number_diff_logfiles = 10;

  // add things like timestamp, threadid, etc... + severity
  log::add_common_attributes();
  log::register_simple_formatter_factory<trivial::severity_level, char>(
    "Severity");

  // create file sink
  auto fsink = boost::make_shared<FileSink>(
          keywords::file_name = "memscrimper_%Y-%m-%d_%H-%M-%S.log",
          keywords::target = "logs",
          keywords::rotation_size = logsize,
          keywords::auto_flush = true,
          keywords::enable_final_rotation = true,  // move the last log file to log-folder
          keywords::open_mode = std::ios_base::out | std::ios_base::app);

  init_file_collecting(fsink, logsize, number_diff_logfiles);
  fsink->locked_backend()->scan_for_files();
  fsink->set_formatter(&log_fmt<false>);
  log::core::get()->add_sink(fsink);

  // create stderr sink
  auto sink = boost::make_shared<text_sink>();
  boost::shared_ptr<std::ostream> stream(&std::cerr, boost::null_deleter());
  sink->locked_backend()->add_stream(stream);
  sink->set_filter(trivial::severity >= trivial::warning);
  sink->set_formatter(&log_fmt<true>);
  log::core::get()->add_sink(sink);

  // create stdout sink
  sink = boost::make_shared<text_sink>();
  stream = boost::shared_ptr<std::ostream>(&std::cout, boost::null_deleter());
  sink->locked_backend()->add_stream(stream);
  if (DEBUGMODE) {
    sink->set_filter(trivial::severity < trivial::warning);
  } else {
    sink->set_filter(trivial::severity == trivial::info);
  }
  sink->set_formatter(&log_fmt<true>);
  log::core::get()->add_sink(sink);
}


void print_help(const std::string &program_name) {
  std::cout << "GENERAL USAGE: \t\t" << program_name
            << " [-h|<c/d/s> <arguments>]\n"
            << "-------------------------------------------------------------\n"
            << "COMPRESS: \t\t" << program_name
            << " c <refdump> <dumpfile> <compressed outfile> <pagesize>\n"
            << "\t\t\t<inner compression> <diffing> <intra>\n"
            << "DECOMPRESS: \t\t" << program_name
            << " d <compressed dumpfile> <uncompressed outfile>\n"
            << "START AS A SERVICE: \t" << program_name << " s <workerthread count> "
            << "<server socket path>\n"
            << "-------------------------------------------------------------\n"
            << "Valid inner compression methods: \n"
            << "'gzip': \tGZIP compression (requires utility gzip/gunzip)\n"
            << "'bzip2': \tBZIP2 compression (requires utility bzip2/bunzip2)\n"
            << "'7zip': \t7ZIP compression (requires utility 7za)\n"
            << "'0': \t\tdisables inner compression\n"
            << "\nValid values for intra/diffing:\n"
            << "'0': \tdisabled intra/diffing\n"
            << "'1': \tenables intra/diffing" << std::endl;
}


int main(int argc, char *argv[]) {
  init_logging();

  // print usage if necessary
  if (argc < 2 || !std::strcmp(argv[1], "-h")) {
    print_help(argv[0]);
    exit(EXIT_SUCCESS);
  }

  // parse compression/decompression/service args
  if (!std::strcmp(argv[1], "c") || !std::strcmp(argv[1], "C")) {
    // check number of arguments
    if (argc != 9) {
      BOOST_LOG_TRIVIAL(error) << "invalid number of arguments";
      exit(EXIT_FAILURE);
    }

    // parse paths + pagesize
    char *ref_path = argv[2];
    char *src_path = argv[3];
    char *out_path = argv[4];
    uint32_t pagesize = atoi(argv[5]);

    // parse inner compression
    mscr::compression inner;
    if (!std::strcmp(argv[6], "bzip2")) {
      inner = mscr::compression::BZIP2;
    } else if (!std::strcmp(argv[6], "gzip")) {
      inner = mscr::compression::GZIP;
    } else if (!std::strcmp(argv[6], "7zip")) {
      inner = mscr::compression::ZIP7;
    } else if (!std::strcmp(argv[6], "0")) {
      inner = mscr::compression::NOINNER;
    } else {
      BOOST_LOG_TRIVIAL(error) << "invalid compression method chosen ("
                               << "valid ones are: bzip2, gzip, 7zip, 0)";
      exit(EXIT_FAILURE);
    }

    // parse diffing/intra
    bool diffing = atoi(argv[7]) == 1;
    bool intra = atoi(argv[8]) == 1;

    // show some info
    BOOST_LOG_TRIVIAL(info) << "compressing\n"
                            << "refpath: " << ref_path << "\n"
                            << "srcpath: " << src_path << "\n"
                            << "outpath: " << out_path << "\n"
                            << "pagesize: " << pagesize << "\n"
                            << "compressing: " << argv[6] << "\n"
                            << "diffing: " << diffing << "\n"
                            << "intra: " << intra;

    // create and read refdump file
    std::shared_ptr<mscr::memdump> ref = std::make_shared<mscr::memdump>(
                        ref_path);
    int ret = ref->readDumpfile(pagesize);
    if (ret) {
      BOOST_LOG_TRIVIAL(error) << "error when reading refdump";
      exit(EXIT_FAILURE);
    }

    // create and read srcdump file
    mscr::memdump srcdump(src_path);
    ret = srcdump.readDumpfile(pagesize);
    if (ret) {
      BOOST_LOG_TRIVIAL(error) << "error when reading srcdump";
      exit(EXIT_FAILURE);
    }

    // compress
    mscr::interdedup_compress(ref, srcdump, out_path, inner, diffing, intra);
  } else if (!std::strcmp(argv[1], "d") || !std::strcmp(argv[1], "D")) {
    // check number of arguments
    if (argc != 4) {
      BOOST_LOG_TRIVIAL(error) << "invalid number of arguments";
      exit(EXIT_FAILURE);
    }

    // parse paths
    char *dump_path = argv[2];
    char *out_path = argv[3];

    // show some info
    BOOST_LOG_TRIVIAL(info) << "compressed dumpfile: " << dump_path << "\n"
                            << "outfile: " << out_path << "\n"
                            << "decompressing";

    // decompress
    mscr::interdedup_decompress(nullptr, dump_path, out_path);
  } else if (!std::strcmp(argv[1], "s") || !std::strcmp(argv[1], "S")) {

    if (argc != 4) {
      BOOST_LOG_TRIVIAL(error) << "invalid number of arguments";
      exit(EXIT_FAILURE);
    }
    // parse workerthread count
    uint32_t thread_count = atoi(argv[2]);
    // parse socket file path
    std::string socket_path(argv[3]);

    BOOST_LOG_TRIVIAL(info) << "starting service";
    mscr::request_handler handler(thread_count);
    try {
      mscr::command_socket mscr_sock(socket_path, &handler);
      mscr_sock.start_listen();
    } catch (std::runtime_error &excp) {
      BOOST_LOG_TRIVIAL(error) << "error occurred: " << excp.what();
      exit(EXIT_FAILURE);
    }
  } else {
    BOOST_LOG_TRIVIAL(error) << "invalid first argument";
    exit(EXIT_FAILURE);
  }

  BOOST_LOG_TRIVIAL(debug) << "removing log sinks";
  /* this prevents crashes caused by boost final file rotation -- more details:
   * https://www.boost.org/doc/libs/1_68_0/libs/log/doc/html/log/rationale/why_crash_on_term.html */
  boost::log::core::get()->remove_all_sinks();

  return EXIT_SUCCESS;
}

