import os
import re

def find_project_dependencies(project_path):
    imports = set()

    pattern = re.compile(r'^\s*(?:import|from)\s+([a-zA-Z0-9_]+)')

    for root, _, files in os.walk(project_path):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)

                with open(file_path, "r", encoding="utf-8") as f:
                    for line in f:
                        match = pattern.match(line)
                        if match:
                            imports.add(match.group(1))

    for lib in sorted(imports):
        print(lib)

if __name__ == "__main__":
    project_path = "/"
    find_project_dependencies(project_path)