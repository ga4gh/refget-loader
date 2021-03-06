import click

@click.command()
@click.argument("processing_dir")
@click.argument("file_id")
@click.argument("source_config")
@click.argument("destination_config")
def manifest(**kwargs):
    "generate an upload manifest from processed ENA flatfile"

    def load_csv(csv_path):
        csv_dict = {}
        columns = []
        parse_header = True
        for line in open(csv_path, "r"):
            ls = line.rstrip().split(",")
            if parse_header:
                columns = ls
                parse_header = False
            else:
                subdict = {columns[i]: ls[i] for i in range(0, len(ls))}
                csv_dict[subdict["trunc512"]] = subdict

        return csv_dict

    processing_dir = kwargs["processing_dir"]
    file_id = kwargs["file_id"]
    logs_dir = processing_dir + "/logs"
    full_csv_path = logs_dir + "/" + file_id + ".full.csv"
    loader_csv_path = logs_dir + "/" + file_id + ".loader.csv"
    full_csv_dict = load_csv(full_csv_path)
    loader_csv_dict = load_csv(loader_csv_path)

    output_manifest_path = logs_dir + "/" + file_id + ".manifest.csv"
    output_content_template = \
        "# Refget loader manifest\n" \
        + "# source config: {}\n" \
        + "# destination config: {}\n" \
        + "{}\n" \
        + "{}\n"
    output_header = [
        "completed", "seq", "metadata", "primary_id", "trunc512_id", "md5_id"
    ]
    output_lines = []

    for trunc512 in loader_csv_dict.keys():
        loader_csv_subdict = loader_csv_dict[trunc512]
        full_csv_subdict = full_csv_dict[trunc512]
        output_lines.append("\t".join([
            loader_csv_subdict["completed"],
            loader_csv_subdict["seq_path"],
            loader_csv_subdict["json_path"],
            full_csv_subdict["ga4gh"],
            loader_csv_subdict["trunc512"],
            loader_csv_subdict["md5"],
        ]))

    # add additional lines for uploading the .full.csv
    output_lines.append("# additional uploads")
    output_lines.append("\t".join(["source", "destination"]))
    output_lines.append("\t".join([
        full_csv_path,
        "metadata/csv/" + file_id + ".full.csv"
    ]))

    output_content = output_content_template.format(
        kwargs["source_config"],
        kwargs["destination_config"],
        "\t".join(output_header),
        "\n".join(output_lines)
    )
    
    open(output_manifest_path, "w").write(output_content)
