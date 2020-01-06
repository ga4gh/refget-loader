import click

@click.command()
@click.argument("processing_dir")
@click.argument("file_id")
@click.argument("source_config")
@click.argument("destination_config")
def manifest(**kwargs):
    "generate an upload manifest from processed ENA flatfile"

    def parse_loader_columns(header_line):
        ls = header_line.rstrip().split(",")
        return {ls[i]: i for i in range(0, len(ls))}

    processing_dir = kwargs["processing_dir"]
    file_id = kwargs["file_id"]
    logs_dir = processing_dir + "/logs"
    loader_csv_path = logs_dir + "/" + file_id + ".loader.csv"
    loader_csv = open(loader_csv_path, "r")

    output_manifest_path = logs_dir + "/" + file_id + ".manifest.csv"
    output_manifest = open(output_manifest_path, "w")

    output_content = [
        "# Refget loader manifest",
        "# source config: " + kwargs["source_config"],
        "# destination config: " + kwargs["destination_config"],
        "\t".join(
            ["completed", "seq", "metadata", "primary_id", "secondary_id_md5"]
        )
    ]

    header = True
    loader_cols = None
    for line in loader_csv.readlines():
        if header:
            loader_cols = parse_loader_columns(line)
            header = False
        else:
            ls = line.rstrip().split(",")
            new_line = "\t".join([
                ls[loader_cols["completed"]],
                ls[loader_cols["seq_path"]],
                ls[loader_cols["json_path"]],
                ls[loader_cols["trunc512"]],
                ls[loader_cols["md5"]]
            ])
            output_content.append(new_line)
    
    output_manifest.write("\n".join(output_content) + "\n")
