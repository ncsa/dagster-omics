from dagster_omics.sensor import parse_url_path_prefix


def test_parse_url_path_prefix():
    # Test case with normal URL as shown in the example
    url = "https://data.nemoarchive.org/biccn/grant/u19_huang/dulac/transcriptome/sncell/10x_multiome/mouse/processed/align/E16F_1_RNA.bam.tar"  # noqa: E501
    expected = "biccn/grant/u19_huang/dulac/transcriptome/sncell/10x_multiome/mouse/processed/align"  # noqa: E501
    assert parse_url_path_prefix(url) == expected
