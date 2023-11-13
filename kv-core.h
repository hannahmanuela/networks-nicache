#ifndef KVCORE_H
#define KVCORE_H

#include <unordered_map>
#include <cstdlib>
#include <cstring>
#include <string>

class KVCore {

  public:
    KVCore() {
      std::unordered_map<std::string, std::string>key_value_map_;
    }

    std::string Get(std::string key) {
      return key_value_map_[key];
    };

    void Put(std::string key, std::string value) {
      key_value_map_.insert(key, value);
    };


  private:
    std::unordered_map<std::string, std::string> key_value_map_;


};

#endif  // KVCORE_H