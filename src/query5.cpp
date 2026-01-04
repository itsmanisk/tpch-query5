#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <bits/stdc++.h>
using namespace std;
// store table path from parseArgs
static string g_table_path;


// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    // TODO: Implement command line argument parsing
    // Example: --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 4 --table_path /path/to/tables --result_path /path/to/results
    for (int i = 1; i < argc; i++) {
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
		    cout<<"Invalid Parsing "<<endl;
		    return false;
	    }
    }
     g_table_path = table_path;
     
     return !(r_name.empty() || start_date.empty() || end_date.empty() || table_path.empty() || result_path.empty() || num_threads <= 0);

}
//split the db data 

static vector<string> split(const string& s, char delim = '|')
{
	vector < string > out;
	string item;
	stringstream ss(s);
	while(getline(ss, item, delim)) 
		out.push_back(item);
	return out;
}


//helper function: load_helper
static bool loadhelper(
    const string& file,
    const vector<string>& cols,
    vector<map<string,string>>& out)
{
	ifstream in (file);
	if(!in.is_open()) return false;
	string line;
	while(getline(in, line)) {
		auto t = split(line);
		map < string, string > row;
		for(size_t i = 0; i < cols.size(); i++) 
			row[cols[i]] = t[i];
		out.push_back(move(row));
	}
	return true;
}



// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string& table_path, std::vector<std::map<std::string, std::string>>& customer_data, std::vector<std::map<std::string, std::string>>& orders_data, std::vector<std::map<std::string, std::string>>& lineitem_data, std::vector<std::map<std::string, std::string>>& supplier_data, std::vector<std::map<std::string, std::string>>& nation_data, std::vector<std::map<std::string, std::string>>& region_data) {
    // TODO: Implement reading TPCH data from files
    return
	loadhelper(table_path + "/customer.tbl", {"c_custkey", "c_name", "c_address", "c_nationkey"}, customer_data) && 
	loadhelper(table_path + "/orders.tbl", {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate"}, orders_data) &&(lineitem_data.clear(), true) && 
	loadhelper(table_path + "/supplier.tbl", {"s_suppkey", "s_name", "s_address", "s_nationkey" }, supplier_data) && 
	loadhelper(table_path + "/nation.tbl", {"n_nationkey", "n_name", "n_regionkey"}, nation_data) && 
	loadhelper(table_path + "/region.tbl", {"r_regionkey", "r_name"}, region_data);
}

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, const std::vector<std::map<std::string, std::string>>& customer_data, const std::vector<std::map<std::string, std::string>>& orders_data, const std::vector<std::map<std::string, std::string>>& lineitem_data, const std::vector<std::map<std::string, std::string>>& supplier_data, const std::vector<std::map<std::string, std::string>>& nation_data, const std::vector<std::map<std::string, std::string>>& region_data, std::map<std::string, double>& results) {
    // TODO: Implement TPCH Query 5 using multithreading
    int regionkey = -1;
	for(const auto & r: region_data)
		if(r.at("r_name") == r_name) 
			regionkey = stoi(r.at("r_regionkey"));
	if(regionkey < 0) return false;
	unordered_map < int, string > nation_name;
	unordered_set < int > valid_nations;
	for(const auto & n: nation_data) {
		if(stoi(n.at("n_regionkey")) == regionkey) {
			int nk = stoi(n.at("n_nationkey"));
			valid_nations.insert(nk);
			nation_name[nk] = n.at("n_name");
		}
	}
	unordered_map < int, int > supplier_to_nation;
	for(const auto & s: supplier_data) {
		int nk = stoi(s.at("s_nationkey"));
		if(valid_nations.count(nk)) 
			supplier_to_nation[stoi(s.at("s_suppkey"))] = nk;
	}
	unordered_map < int, int > customer_to_nation;
	for(const auto & c: customer_data) {
		int nk = stoi(c.at("c_nationkey"));
		if(valid_nations.count(nk)) 
			customer_to_nation[stoi(c.at("c_custkey"))] = nk;
	}
	unordered_map < int, int > order_to_nation;
	for(const auto & o: orders_data) {
		const string & d = o.at("o_orderdate");
		if(d >= start_date && d < end_date) {
			int ck = stoi(o.at("o_custkey"));
			auto it = customer_to_nation.find(ck);
			if(it != customer_to_nation.end()) 
				order_to_nation[stoi(o.at("o_orderkey"))] = it-> second;
		}
	}
	unordered_map < int, double > revenue_by_nation;
	ifstream in (g_table_path + "/lineitem.tbl");
	if(!in.is_open()) return false;
	string line;
	while(getline(in, line)) {
		auto t = split(line);
		if(t.size() < 7) continue;
		int orderkey = stoi(t[0]);
		int suppkey = stoi(t[2]);
		auto oit = order_to_nation.find(orderkey);
		if(oit == order_to_nation.end()) continue;
		auto sit = supplier_to_nation.find(suppkey);
		if(sit == supplier_to_nation.end()) continue;
		double price = stod(t[5]);
		double disc = stod(t[6]);
		revenue_by_nation[sit->second] += price * (1.0 - disc);
	}
	for(auto & [nk, rev]: revenue_by_nation) 
		results[nation_name[nk]] += rev;
	return true;
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    // TODO: Implement outputting results to a file
    ofstream out(result_path);
	if(!out.is_open()) return false;
	for(auto & [n, r]: results) 
		out << n << " | " << fixed << setprecision(2) << r << "\n";
	return true;
} 
