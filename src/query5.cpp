#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>

using namespace std;
// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    // TODO: Implement command line argument parsing
    // Example: --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 4 --table_path /path/to/tables --result_path /path/to/results
    for (int i = 1; i < argc; i++) {
	    //cout<<"Args: "<<argv[i]<<endl;
	    string arg = argv[i];
	    if (arg == "--r_name" && i + 1 < argc)
		    r_name = argv[++i];
	    else if (arg == "--start_date" && i + 1 < argc)
		    start_date = argv[++i];
	    else if (arg == "--end_date" && i + 1 < argc)
		    end_date = argv[++i];
	    else if (arg == "--threads" && i + 1 < argc)
		    num_threads = stoi(argv[++i]);
	    else if (arg == "--table_path" && i + 1 < argc)
		    table_path = argv[++i];
	    else if (arg == "--result_path" && i + 1 < argc)
		    result_path = argv[++i];
	    else {
		    cout << "Unknown or incomplete argument: " << arg << "\n";
		    return false;
	    }
    }
    //cout<<r_name<<start_date<<end_date<<table_path<<result_path<<num_threads<<endl;
    return !(r_name.empty() || start_date.empty() || end_date.empty() || table_path.empty() || result_path.empty() || num_threads <= 0);

    //return false;
}
//split the db data 

static vector<string> split(const string& s, char delim) {
	vector<string> tokens;
	stringstream ss(s);
	string item;
	while (getline(ss, item, delim))
		tokens.push_back(item);
	return tokens;
}


//helper function: load_helper
static bool loadhelper(
    const string& file,
    const vector<string>& cols,
    vector<map<string,string>>& out)
{
	ifstream in(file);
	if (!in.is_open()) {
		cout << "Failed to open " << file << "\n";
		return false;
	}
	
	string line;
	while (getline(in, line)) {
		auto tokens = split(line, '|');
		map<string, string> row;
		
		for (size_t i = 0; i < cols.size(); i++)
			row[cols[i]] = tokens[i];
		out.push_back(move(row));
	}
	return true;
}



// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string& table_path, std::vector<std::map<std::string, std::string>>& customer_data, std::vector<std::map<std::string, std::string>>& orders_data, std::vector<std::map<std::string, std::string>>& lineitem_data, std::vector<std::map<std::string, std::string>>& supplier_data, std::vector<std::map<std::string, std::string>>& nation_data, std::vector<std::map<std::string, std::string>>& region_data) {
    // TODO: Implement reading TPCH data from files
    return
	    loadhelper(table_path + "/customer.tbl",{"c_custkey", "c_name", "c_address", "c_nationkey"},customer_data) &&
	    loadhelper(table_path + "/orders.tbl",{"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate"},orders_data) &
	    loadhelper(table_path + "/lineitem.tbl",{"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount"},lineitem_data) && 
	    loadhelper(table_path + "/supplier.tbl", {"s_suppkey", "s_name", "s_address", "s_nationkey"}, supplier_data) &&
	    loadhelper(table_path + "/nation.tbl", {"n_nationkey", "n_name", "n_regionkey"}, nation_data) &&
	    loadhelper(table_path + "/region.tbl", {"r_regionkey", "r_name"}, region_data);
}

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, const std::vector<std::map<std::string, std::string>>& customer_data, const std::vector<std::map<std::string, std::string>>& orders_data, const std::vector<std::map<std::string, std::string>>& lineitem_data, const std::vector<std::map<std::string, std::string>>& supplier_data, const std::vector<std::map<std::string, std::string>>& nation_data, const std::vector<std::map<std::string, std::string>>& region_data, std::map<std::string, double>& results) {
    // TODO: Implement TPCH Query 5 using multithreading
    return false;
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    // TODO: Implement outputting results to a file
    return false;
} 
