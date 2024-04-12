import yaml

def yaml_to_tree(yaml_data, indent=0, prefix=''):
    """
    Recursively converts YAML data to a tree structure string.
    :param yaml_data: Parsed YAML data.
    :param indent: Current indentation level.
    :param prefix: Prefix for the current line in the tree.
    :return: String representation of the YAML data in tree structure.
    """
    tree = ""
    items = list(yaml_data.items())
    
    for i, (key, value) in enumerate(items):
        spacer = '   ' * indent
        # Use '└── ' for the last item and '├── ' for others
        connector = '└── ' if i == len(items) - 1 else '├── '
        
        tree += f"{prefix}{spacer}{connector}{key}\n"
        if isinstance(value, dict):
            new_prefix = prefix + spacer + ('   ' if i == len(items) - 1 else '│  ')
            tree += yaml_to_tree(value, indent + 1, new_prefix)
    return tree


# Example YAML content
yaml_content = "v0.2/config.yaml"

# Parse the YAML content
with open(yaml_content, 'r', encoding="utf-8") as file:
    parsed_yaml = yaml.safe_load(file)

# Generate the tree structure
tree_structure = yaml_to_tree(parsed_yaml)

print(tree_structure)