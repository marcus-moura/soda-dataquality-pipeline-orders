def get_file_path(file_name: str):
    import os
    # Check if current dir is app
    if os.path.basename(os.getcwd()) == 'app':
        return f"input_data/{file_name}"
    return f"app/input_data/{file_name}"