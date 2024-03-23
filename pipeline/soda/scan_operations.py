from loguru import logger

def run_soda_scan(data_source, scan_name, project_root='soda', checks_subpath = None):
    from soda.scan import Scan

    config_file = f"{project_root}/configuration.yml"
    checks_path = f"{project_root}/checks"

    if checks_subpath:
        checks_path += f"/{checks_subpath}"

    scan = Scan()
    scan.set_verbose()
    scan.add_configuration_yaml_file(config_file)
    scan.set_data_source_name(data_source)
    scan.add_sodacl_yaml_files(checks_path)
    scan.set_scan_definition_name(scan_name)

    result = scan.execute()
    logger.info(scan.get_logs_text())

    if result != 0:
        raise ValueError('Soda Scan failed')

    return result