if ( !file(params.kmer_db{{ param_id }}).exists() ) {
    exit 1, "'kmer_db{{ param_id }}' database was not found: '${params.kmer_db{{ param_id }}}'"
}

IN_kmer_db_{{ pid }} = Channel.fromPath(params.kmer_db{{ param_id }})

process mentalist_{{ pid }} {

    // Send POST request to platform
    {% include "post.txt" ignore missing %}

    tag { sample_id }

    input:
    set sample_id, file(fastq_pair) from {{ input_channel }}
    val kmer_db from IN_kmer_db_{{ pid }}

    output:
    file("${sample_id}_mentalist.txt")
    {% with task_name="mentalist" %}
    {%- include "compiler_channels.txt" ignore missing -%}
    {% endwith %}

    script:
    """
    mentalist call -o ''${sample_id}_mentalist.txt' --db '$kmer_db' -s $sample_id $fastq_pair
    """
}

{{ forks }}
