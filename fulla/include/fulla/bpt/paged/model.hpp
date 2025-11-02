/*
 * File: model.hpp
 * Author: newenclave
 * GitHub: https://github.com/newenclave
 * Created: 2025-11-02
 * License: MIT
 */


#pragma once

#include <optional>

#include "fulla/bpt/concepts.hpp"
#include "fulla/page/header.hpp"
#include "fulla/page/slot_page.hpp"
#include "fulla/storage/device.hpp"

namespace fulla::bpt::paged {

    template <storage::RandomAccessDevice DeviceT, typename PidT = std::uint32_t>
    struct model {

        using buffer_manager_type = storage::buffer_manager<DeviceT, PidT>;
        using node_id_type = PidT;

        struct node_base {
            virtual ~node_base() = default;
        };

        struct leaf_type {};
        struct inode_type {};

        model(buffer_manager_type& mgr)
            : allocator_(mgr) 
        {}

        struct allocator_type {

            allocator_type(buffer_manager_type& mgr)
                : mgr_(mgr) 
            {}

            leaf_type create_leaf() {
                return {};
            }
            
            inode_type create_inode() {
                return {};
            }

            void destroy(node_id_type id) {
                // no-op
            }

            leaf_type load_leaf(node_id_type id) {
                return {};
            }
            
            inode_type load_inode(node_id_type id) {
                return {};
            }

            std::tuple<node_id_type, bool> load_root() {
                if(root_) {
                    return { *root_, true };
                }
                return { {}, false };
            }

            void set_root(mode_id_type id) {
                root_ = {id};
            }

            std::optional<node_id_type> root_ {};
            buffer_manager_type mgr_;
        };

        allocator_type &get_allocator() {
            return allocator_;
        }
        
        const allocator_type &get_allocator() const {
            return allocator_;
        }

    private:
        allocator_type allocator_;
    };

}