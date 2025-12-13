#include "fulla/core/types.hpp"
#include "root.hpp"
#include "fulla/storage/file_block_device.hpp"

#include "CLI11/CLI11.hpp"
#include "replxx.hxx"

#include <iostream>
#include <string>
#include <sstream>

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

	std::vector<std::string> split_command(const std::string& line) {
		std::vector<std::string> args;
		std::string current;
		bool in_quotes = false;
		bool escaped = false;

		for (char ch : line) {
			if (escaped) {
				current += ch;
				escaped = false;
			}
			else if (ch == '\\') {
				escaped = true;
			}
			else if (ch == '"') {
				in_quotes = !in_quotes;
			}
			else if (std::isspace(static_cast<unsigned char>(ch)) && !in_quotes) {
				if (!current.empty()) {
					args.push_back(current);
					current.clear();
				}
			}
			else {
				current += ch;
			}
		}

		if (!current.empty()) {
			args.push_back(current);
		}

		return args;
	}

	// Helper: Navigate to parent directory of a path
	template<typename DirHandle>
	std::pair<DirHandle, std::string> navigate_to_parent(DirHandle root_dir, const std::string& path) {
		auto path_components = split_path(path);
		if (path_components.empty()) {
			return { std::move(root_dir), "" };
		}

		std::string filename = path_components.back();
		path_components.pop_back();

		auto current_dir = std::move(root_dir);
		for (const auto& component : path_components) {
			auto itr = current_dir.find(component);
			if (itr != current_dir.end() && itr->is_directory()) {
				current_dir = itr->handle();
			}
			else {
				return { DirHandle{}, "" }; // Invalid path
			}
		}

		return { std::move(current_dir), filename };
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
				else {
					std::cerr << "Directory not found: " << path_components[i] << "\n";
					return 1;
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

			auto [parent_dir, file_name] = navigate_to_parent(std::move(dir), path);
			if (!parent_dir.is_valid()) {
				std::cerr << "Parent directory not found\n";
				return 1;
			}

			auto new_file = parent_dir.touch(file_name);
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
			std::cerr << "(File writing API needs to be exposed in file_handle)\n";
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

			auto dir = root.open_root();
			if (!dir.is_valid()) {
				std::cerr << "Failed to open root directory\n";
				return 1;
			}

			auto [parent_dir, file_name] = navigate_to_parent(std::move(dir), path);
			if (!parent_dir.is_valid()) {
				std::cerr << "Parent directory not found\n";
				return 1;
			}

			auto file = parent_dir.open_file(file_name);
			if (file.is_valid()) {
				auto hdl = file.open();
				if (hdl.is_valid()) {
					std::string data(128, '\n');
					while (!hdl.is_endg()) {
						auto read = hdl.read(reinterpret_cast<core::byte *>(data.data()), data.size());
						std::cout.write(data.data(), read);
					}
				}
				return 0;
			}

			std::cerr << "Failed to create file\n";
			return 1;


			return 1;
		}
		catch (const std::exception& e) {
			std::cerr << "Error reading file: " << e.what() << "\n";
			return 1;
		}
	}

	int cmd_rm(const std::string& filename, const std::string& path) {
		try {
			block_device_type dev(filename, DEFAULT_PAGE_SIZE);
			root_type root(dev, DEFAULT_CACHE_SIZE);

			std::cerr << "Remove command not yet implemented\n";
			std::cerr << "(Delete API needs to be added to directory_handle)\n";
			return 1;
		}
		catch (const std::exception& e) {
			std::cerr << "Error removing entry: " << e.what() << "\n";
			return 1;
		}
	}

	int cmd_stat(const std::string& filename, const std::string& path) {
		try {
			block_device_type dev(filename, DEFAULT_PAGE_SIZE);
			root_type root(dev, DEFAULT_CACHE_SIZE);

			auto dir = root.open_root();
			if (!dir.is_valid()) {
				std::cerr << "Failed to open root directory\n";
				return 1;
			}

			auto [parent_dir, entry_name] = navigate_to_parent(std::move(dir), path);
			if (!parent_dir.is_valid()) {
				std::cerr << "Parent directory not found\n";
				return 1;
			}

			auto itr = parent_dir.find(entry_name);
			if (itr == parent_dir.end()) {
				std::cerr << "Entry not found: " << path << "\n";
				return 1;
			}

			std::cout << "Path: " << path << "\n";
			std::cout << "Name: " << itr->name() << "\n";
			std::cout << "Type: " << (itr->is_directory() ? "Directory" : "File") << "\n";
			std::cout << "Page ID: " << itr->page_id() << "\n";

			return 0;
		}
		catch (const std::exception& e) {
			std::cerr << "Error getting stats: " << e.what() << "\n";
			return 1;
		}
	}

	int cmd_tree(const std::string& filename, const std::string& path, int indent = 0) {
		try {
			block_device_type dev(filename, DEFAULT_PAGE_SIZE);
			root_type root(dev, DEFAULT_CACHE_SIZE);

			auto dir = root.open_root();
			if (!dir.is_valid()) {
				std::cerr << "Failed to open root directory\n";
				return 1;
			}

			// Navigate to target directory
			auto path_components = split_path(path);
			auto current_dir = std::move(dir);

			for (const auto& component : path_components) {
				auto itr = current_dir.find(component);
				if (itr != current_dir.end() && itr->is_directory()) {
					current_dir = itr->handle();
				}
				else {
					std::cerr << "Directory not found: " << component << "\n";
					return 1;
				}
			}

			// Print tree
			std::function<void(decltype(current_dir)&, int)> print_tree;
			print_tree = [&](auto& d, int level) {
				for (const auto& entry : d) {
					std::cout << std::string(level * 2, ' ')
						<< (entry.is_directory() ? "DIR " : "FIL ")
						<< entry.name() << "\n";

					if (entry.is_directory()) {
						auto subdir = entry.handle();
						if (subdir.is_valid()) {
							print_tree(subdir, level + 1);
						}
					}
				}
				};

			std::cout << ".\n";
			print_tree(current_dir, 1);

			return 0;
		}
		catch (const std::exception& e) {
			std::cerr << "Error displaying tree: " << e.what() << "\n";
			return 1;
		}
	}

	void cmd_help() {
		std::cout << "\nFullaFS Available Commands:\n";
		std::cout << "  format          - Format the filesystem\n";
		std::cout << "  ls [path]       - List directory contents (default: /)\n";
		std::cout << "  mkdir <path>    - Create directory (recursive)\n";
		std::cout << "  touch <path>    - Create a file\n";
		std::cout << "  stat <path>     - Show entry information\n";
		std::cout << "  tree [path]     - Display directory tree\n";
		std::cout << "  rm <path>       - Remove file/directory (TODO)\n";
		std::cout << "  cat <path>      - Display file contents (TODO)\n";
		std::cout << "  echo <path> <text> - Write to file (TODO)\n";
		std::cout << "  help            - Show this help\n";
		std::cout << "  exit/quit       - Exit shell\n\n";
	}
}

void shell_mode(const std::string& fs_file) {
	replxx::Replxx rx;
	rx.set_max_history_size(128);

	std::cout << "FullaFS Shell - " << fs_file << "\n";
	std::cout << "Type 'help' for commands, 'exit' to quit\n\n";

	while (true) {
		const char* input = rx.input("fullafs> ");
		if (!input) break;

		std::string line(input);
		if (line.empty()) continue;
		if (line == "exit" || line == "quit") break;

		std::vector<std::string> args = split_command(line);
		if (args.empty()) continue;

		const auto& cmd = args[0];

		if (cmd == "help") {
			cmd_help();
		}
		else if (cmd == "format") {
			cmd_format(fs_file);
		}
		else if (cmd == "ls") {
			cmd_ls(fs_file, args.size() > 1 ? args[1] : "/");
		}
		else if (cmd == "mkdir") {
			if (args.size() > 1) {
				cmd_mkdir(fs_file, args[1]);
			}
			else {
				std::cerr << "Usage: mkdir <path>\n";
			}
		}
		else if (cmd == "touch") {
			if (args.size() > 1) {
				cmd_touch(fs_file, args[1]);
			}
			else {
				std::cerr << "Usage: touch <path>\n";
			}
		}
		else if (cmd == "stat") {
			if (args.size() > 1) {
				cmd_stat(fs_file, args[1]);
			}
			else {
				std::cerr << "Usage: stat <path>\n";
			}
		}
		else if (cmd == "tree") {
			cmd_tree(fs_file, args.size() > 1 ? args[1] : "/");
		}
		else if (cmd == "rm") {
			if (args.size() > 1) {
				cmd_rm(fs_file, args[1]);
			}
			else {
				std::cerr << "Usage: rm <path>\n";
			}
		}
		else if (cmd == "cat") {
			if (args.size() > 1) {
				cmd_cat(fs_file, args[1]);
			}
			else {
				std::cerr << "Usage: cat <path>\n";
			}
		}
		else if (cmd == "echo") {
			if (args.size() > 2) {
				cmd_echo(fs_file, args[1], args[2]);
			}
			else {
				std::cerr << "Usage: echo <path> <content>\n";
			}
		}
		else {
			std::cerr << "Unknown command: " << cmd << " (type 'help' for available commands)\n";
		}

		rx.history_add(line);
	}
}

int main([[maybe_unused]] int argc, [[maybe_unused]] char* argv[]) {
	CLI::App app{ "FullaFS - Filesystem Command Line Tool" };

	std::string filename;
	app.add_option("filesystem", filename, "Filesystem file (.bin)")->required();

	app.require_subcommand(1);

	std::string path;
	std::string content;

	auto shell_cmd = app.add_subcommand("shell", "Interactive shell mode");
	shell_cmd->callback([&]() {
		shell_mode(filename);
		return 0;
		});

	auto format_cmd = app.add_subcommand("format", "Format a new filesystem");
	format_cmd->callback([&]() {
		return cmd_format(filename);
		});

	auto ls_cmd = app.add_subcommand("ls", "List directory contents");
	ls_cmd->add_option("path", path, "Directory path (default: /)")->default_val("/");
	ls_cmd->callback([&]() {
		return cmd_ls(filename, path);
		});

	auto mkdir_cmd = app.add_subcommand("mkdir", "Create a directory");
	mkdir_cmd->add_option("path", path, "Directory path")->required();
	mkdir_cmd->callback([&]() {
		return cmd_mkdir(filename, path);
		});

	auto touch_cmd = app.add_subcommand("touch", "Create a file");
	touch_cmd->add_option("path", path, "File path")->required();
	touch_cmd->callback([&]() {
		return cmd_touch(filename, path);
		});

	auto stat_cmd = app.add_subcommand("stat", "Show entry information");
	stat_cmd->add_option("path", path, "Entry path")->required();
	stat_cmd->callback([&]() {
		return cmd_stat(filename, path);
		});

	auto tree_cmd = app.add_subcommand("tree", "Display directory tree");
	tree_cmd->add_option("path", path, "Directory path (default: /)")->default_val("/");
	tree_cmd->callback([&]() {
		return cmd_tree(filename, path);
		});

	auto echo_cmd = app.add_subcommand("echo", "Write content to file");
	echo_cmd->add_option("path", path, "File path")->required();
	echo_cmd->add_option("content", content, "Content to write")->required();
	echo_cmd->callback([&]() {
		return cmd_echo(filename, path, content);
		});

	auto cat_cmd = app.add_subcommand("cat", "Display file contents");
	cat_cmd->add_option("path", path, "File path")->required();
	cat_cmd->callback([&]() {
		return cmd_cat(filename, path);
		});

	CLI11_PARSE(app, argc, argv);

	return 0;
}
