#include "fulla/core/types.hpp"
#include "root.hpp"
#include "fulla/storage/file_block_device.hpp"

#include "CLI11/CLI11.hpp"

#include <iostream>
#include <string>

namespace {
	using namespace fullafs;
	using block_device_type = fulla::storage::file_block_device;
	using root_type = root<block_device_type>;

	constexpr std::size_t DEFAULT_PAGE_SIZE = 4096;
	constexpr std::size_t DEFAULT_CACHE_SIZE = 10;

	std::vector<std::string> split_path(const std::string& path_str) {
		std::filesystem::path p(path_str);
		std::vector<std::string> components;

		for (const auto& part : p) {
			std::string component = part.string();

			if (component != "/" && component != "\\" && !component.empty()) {
				components.push_back(component);
			}
		}

		return components;
	}

	std::string normalize_path(const std::string& path) {
		return path;
	}

	int cmd_format(const std::string& filename) {
		try {
			block_device_type dev(filename, DEFAULT_PAGE_SIZE);
			root_type root(dev, DEFAULT_CACHE_SIZE);
			root.format();
			std::cout << "Filesystem formatted: " << filename << "\n";
			return 0;
		}
		catch (const std::exception& e) {
			std::cerr << "Error formatting filesystem: " << e.what() << "\n";
			return 1;
		}
	}

	int cmd_ls(const std::string& filename, const std::string& path) {
		try {
			block_device_type dev(filename, DEFAULT_PAGE_SIZE);
			root_type root(dev, DEFAULT_CACHE_SIZE);

			auto dir = root.open_root();
			if (!dir.is_valid()) {
				std::cerr << "Failed to open root directory\n";
				return 1;
			}

			auto path_components = split_path(path);
			auto current_dir = std::move(dir);

			for (std::size_t i = 0; i < path_components.size(); ++i) {
				auto itr = current_dir.find(path_components[i]);
				if (itr != current_dir.end() && itr->is_directory()) {
					current_dir = itr->handle();
				}
			}

			std::cout << "Total entries: " << current_dir.total_entries() << "\n";
			for (const auto& entry : current_dir) {
				std::cout << (entry.is_directory() ? "DIR  " : "FILE ")
					<< entry.name() << "\n";
			}

			return 0;
		}
		catch (const std::exception& e) {
			std::cerr << "Error listing directory: " << e.what() << "\n";
			return 1;
		}
	}

	int cmd_mkdir(const std::string& filename, const std::string& path) {
		try {
			block_device_type dev(filename, DEFAULT_PAGE_SIZE);
			root_type root(dev, DEFAULT_CACHE_SIZE);

			auto dir = root.open_root();
			if (!dir.is_valid()) {
				std::cerr << "Failed to open root directory\n";
				return 1;
			}

			auto path_components = split_path(path);
			if (path_components.empty()) {
				std::cerr << "Invalid path\n";
				return 1;
			}

			auto current_dir = std::move(dir);
			for (std::size_t i = 0; i < path_components.size(); ++i) {

				auto itr = current_dir.find(path_components[i]);
				if (itr != current_dir.end() && itr->is_directory()) {
					current_dir = itr->handle();
				}
				else {
					auto new_dir = current_dir.mkdir(path_components[i]);
					if (!new_dir.is_valid()) {
						std::cerr << "Failed to create directory: " << path_components[i] << "\n";
						return 1;
					}
					current_dir = std::move(new_dir);
				}
			}

			root.get_allocator().flush_all();
			std::cout << "Directory created: " << path << "\n";
			return 0;
		}
		catch (const std::exception& e) {
			std::cerr << "Error creating directory: " << e.what() << "\n";
			return 1;
		}
	}

	int cmd_touch(const std::string& filename, const std::string& path) {
		try {
			block_device_type dev(filename, DEFAULT_PAGE_SIZE);
			root_type root(dev, DEFAULT_CACHE_SIZE);

			auto dir = root.open_root();
			if (!dir.is_valid()) {
				std::cerr << "Failed to open root directory\n";
				return 1;
			}

			auto new_file = dir.touch(path);
			if (new_file.is_valid()) {
				root.get_allocator().flush_all();
				std::cout << "File created: " << path << "\n";
				return 0;
			}

			std::cerr << "Failed to create file\n";
			return 1;
		}
		catch (const std::exception& e) {
			std::cerr << "Error creating file: " << e.what() << "\n";
			return 1;
		}
	}

	int cmd_echo(const std::string& filename, const std::string& path, const std::string& content) {
		try {
			block_device_type dev(filename, DEFAULT_PAGE_SIZE);
			root_type root(dev, DEFAULT_CACHE_SIZE);

			std::cerr << "Echo command not yet fully implemented\n";
			return 1;
		}
		catch (const std::exception& e) {
			std::cerr << "Error writing to file: " << e.what() << "\n";
			return 1;
		}
	}

	int cmd_cat(const std::string& filename, const std::string& path) {
		try {
			block_device_type dev(filename, DEFAULT_PAGE_SIZE);
			root_type root(dev, DEFAULT_CACHE_SIZE);

			std::cerr << "Cat command not yet fully implemented\n";
			return 1;
		}
		catch (const std::exception& e) {
			std::cerr << "Error reading file: " << e.what() << "\n";
			return 1;
		}
	}
}

int main([[maybe_unused]] int argc, [[maybe_unused]] char* argv[]) {
	CLI::App app{ "FullaFS - Filesystem Command Line Tool" };
	app.require_subcommand(1);

	std::string filename;
	std::string path;
	std::string content;

	// Format subcommand
	auto format_cmd = app.add_subcommand("format", "Format a new filesystem");
	format_cmd->add_option("file", filename, "Filesystem file (.bin)")->required();
	format_cmd->callback([&]() {
		return cmd_format(filename);
		});

	// List directory subcommand
	auto ls_cmd = app.add_subcommand("ls", "List directory contents");
	ls_cmd->add_option("path", path, "Directory path (default: /)")->default_val("/");
	ls_cmd->add_option("file", filename, "Filesystem file (.bin)")->required();
	ls_cmd->callback([&]() {
		return cmd_ls(filename, path);
		});

	// Make directory subcommand
	auto mkdir_cmd = app.add_subcommand("mkdir", "Create a directory");
	mkdir_cmd->add_option("path", path, "Directory path")->required();
	mkdir_cmd->add_option("file", filename, "Filesystem file (.bin)")->required();
	mkdir_cmd->callback([&]() {
		return cmd_mkdir(filename, path);
		});

	// Touch file subcommand
	auto touch_cmd = app.add_subcommand("touch", "Create a file");
	touch_cmd->add_option("path", path, "File path")->required();
	touch_cmd->add_option("file", filename, "Filesystem file (.bin)")->required();
	touch_cmd->callback([&]() {
		return cmd_touch(filename, path);
		});

	// Echo to file subcommand
	auto echo_cmd = app.add_subcommand("echo", "Write content to file");
	echo_cmd->add_option("path", path, "File path")->required();
	echo_cmd->add_option("content", content, "Content to write")->required();
	echo_cmd->add_option("file", filename, "Filesystem file (.bin)")->required();
	echo_cmd->callback([&]() {
		return cmd_echo(filename, path, content);
		});

	// Cat file subcommand
	auto cat_cmd = app.add_subcommand("cat", "Display file contents");
	cat_cmd->add_option("path", path, "File path")->required();
	cat_cmd->add_option("file", filename, "Filesystem file (.bin)")->required();
	cat_cmd->callback([&]() {
		return cmd_cat(filename, path);
		});

	CLI11_PARSE(app, argc, argv);

	return 0;
}
