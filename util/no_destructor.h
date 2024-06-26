//
// Created by 王洪城 on 2024/4/29.
//

#ifndef LEVELDB_COPY_NO_DESTRUCTOR_H
#define LEVELDB_COPY_NO_DESTRUCTOR_H

#include "type_traits"
#include "utility"

namespace leveldb {

template <typename InstanceType>
class NoDestructor {
public:
    template <typename... ConstructorArgTypes>
    explicit NoDestructor(ConstructorArgTypes&&... constructor_args) {
        static_assert(sizeof(instance_storage_) >= sizeof(InstanceType), "instance_storage_ is not large enough");
        static_assert(alignof(decltype(instance_storage_)) >= alignof(InstanceType), "instance_storage_ does not meet alignment requirements");
        new (&instance_storage_) InstanceType(std::forward<ConstructorArgTypes>(constructor_args)...);
    }
    ~NoDestructor() = default;
    NoDestructor(const NoDestructor&) = delete;
    NoDestructor& operator=(const NoDestructor&) = delete;
    InstanceType* get() {
        return reinterpret_cast<InstanceType*>(&instance_storage_);
    }
private:
    typename std::aligned_storage<sizeof(InstanceType), alignof(InstanceType)>::type instance_storage_;
};
} // namespace leveldb
#endif //LEVELDB_COPY_NO_DESTRUCTOR_H
