"""
Bioinformatics Analysis Pipeline with Slurm Executor

This example demonstrates a typical bioinformatics workflow processing genomic data
with different computational requirements for each step.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "bioinformatics-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    "bioinformatics_slurm_pipeline",
    default_args=default_args,
    description="Bioinformatics analysis pipeline using Slurm executor",
    schedule_interval="0 4 * * *",  # Daily at 4 AM
    catchup=False,
    max_active_runs=1,
    tags=["slurm", "bioinformatics", "genomics", "analysis"],
)


def check_input_files():
    """Check if input sequencing files are available."""
    import os
    import random

    # Simulate checking for new sequencing data
    sample_files = [
        "sample_001_R1.fastq.gz",
        "sample_001_R2.fastq.gz",
        "sample_002_R1.fastq.gz",
        "sample_002_R2.fastq.gz",
    ]

    # Simulate file availability (80% chance)
    files_available = random.random() > 0.2

    if files_available:
        print(f"✓ Found {len(sample_files)} sequencing files to process")
        return "setup_workspace"
    else:
        print("✗ No new sequencing files found")
        return "no_files_to_process"


# Start
start = DummyOperator(task_id="start", dag=dag)

# Check for input files
check_files = BranchPythonOperator(
    task_id="check_input_files",
    python_callable=check_input_files,
    executor_config={
        "slurm": {
            "partition": "normal",
            "cpus_per_task": 1,
            "mem": "256M",
            "time_limit": "00:02:00",
        }
    },
    dag=dag,
)

# Skip processing if no files
no_files = DummyOperator(task_id="no_files_to_process", dag=dag)

# Setup workspace
setup_workspace = BashOperator(
    task_id="setup_workspace",
    bash_command="""
    echo "=== Setting up Bioinformatics Workspace ==="
    
    WORK_DIR="/tmp/bioinformatics_{{ ds_nodash }}"
    mkdir -p "$WORK_DIR"/{raw_data,quality_control,aligned_data,variants,reports,references}
    
    echo "Workspace created: $WORK_DIR"
    
    # Simulate downloading reference genome
    echo "Downloading reference genome..."
    REF_DIR="$WORK_DIR/references"
    
    # Create mock reference files
    echo ">chr1" > "$REF_DIR/reference.fasta"
    echo "ATCGATCGATCGATCG$(head -c 1000 /dev/urandom | tr -dc 'ATCG')" >> "$REF_DIR/reference.fasta"
    echo ">chr2" >> "$REF_DIR/reference.fasta"
    echo "GCTAGCTAGCTAGCTA$(head -c 1000 /dev/urandom | tr -dc 'ATCG')" >> "$REF_DIR/reference.fasta"
    
    # Create mock sequencing data
    echo "Creating sample sequencing data..."
    for sample in 001 002; do
        for read in R1 R2; do
            # Create mock FASTQ files
            FASTQ_FILE="$WORK_DIR/raw_data/sample_${sample}_${read}.fastq"
            echo "Creating $FASTQ_FILE"
            
            for i in $(seq 1 1000); do
                echo "@read_${sample}_${read}_${i}"
                echo "$(head -c 100 /dev/urandom | tr -dc 'ATCG')"
                echo "+"
                echo "$(head -c 100 /dev/urandom | tr -dc 'IIII')"
            done > "$FASTQ_FILE"
            
            # Compress
            gzip "$FASTQ_FILE"
        done
    done
    
    echo "Workspace setup completed"
    echo "Files created:"
    find "$WORK_DIR" -type f | head -10
    """,
    executor_config={
        "slurm": {
            "partition": "normal",
            "cpus_per_task": 1,
            "mem": "1G",
            "time_limit": "00:05:00",
        }
    },
    dag=dag,
)

# Quality control - parallel processing of samples
qc_tasks = []
for sample in ["001", "002"]:
    qc_task = BashOperator(
        task_id=f"quality_control_sample_{sample}",
        bash_command=f"""
        echo "=== Quality Control for Sample {sample} ==="
        
        WORK_DIR="/tmp/bioinformatics_{{{{ ds_nodash }}}}"
        SAMPLE_DIR="$WORK_DIR/quality_control/sample_{sample}"
        mkdir -p "$SAMPLE_DIR"
        
        # Simulate FastQC analysis
        echo "Running FastQC on sample {sample}..."
        
        for read in R1 R2; do
            INPUT_FILE="$WORK_DIR/raw_data/sample_{sample}_${{read}}.fastq.gz"
            OUTPUT_FILE="$SAMPLE_DIR/sample_{sample}_${{read}}_fastqc.txt"
            
            if [ -f "$INPUT_FILE" ]; then
                echo "Processing $INPUT_FILE"
                
                # Simulate quality metrics
                TOTAL_READS=$(zcat "$INPUT_FILE" | wc -l | awk '{{print $1/4}}')
                AVG_QUALITY=$(echo "scale=2; 30 + $RANDOM % 10" | bc)
                GC_CONTENT=$(echo "scale=2; 45 + $RANDOM % 10" | bc)
                
                # Generate QC report
                {{
                    echo "FastQC Report for sample_{sample}_${{read}}"
                    echo "=================================="
                    echo "Total Reads: $TOTAL_READS"
                    echo "Average Quality: $AVG_QUALITY"
                    echo "GC Content: $GC_CONTENT%"
                    echo "Status: PASS"
                    echo "Generated: $(date)"
                }} > "$OUTPUT_FILE"
                
                echo "QC completed for ${{read}} read"
            else
                echo "Warning: Input file not found: $INPUT_FILE"
            fi
        done
        
        # Generate sample summary
        SUMMARY_FILE="$SAMPLE_DIR/summary.txt"
        {{
            echo "Sample {sample} Quality Control Summary"
            echo "======================================"
            echo "Date: {{{{ ds }}}}"
            echo "Files processed:"
            ls -la "$WORK_DIR/raw_data/sample_{sample}"*
            echo "Quality reports generated:"
            ls -la "$SAMPLE_DIR"/*.txt
        }} > "$SUMMARY_FILE"
        
        echo "Quality control completed for sample {sample}"
        """,
        executor_config={
            {
                "slurm": {
                    {
                        "partition": "normal",
                        "cpus_per_task": 2,
                        "mem": "2G",
                        "time_limit": "00:15:00",
                    }
                }
            }
        },
        dag=dag,
    )
    qc_tasks.append(qc_task)

# Read alignment - memory intensive
alignment_tasks = []
for sample in ["001", "002"]:
    align_task = BashOperator(
        task_id=f"align_sample_{sample}",
        bash_command=f"""
        echo "=== Aligning Sample {sample} ==="
        
        WORK_DIR="/tmp/bioinformatics_{{{{ ds_nodash }}}}"
        ALIGN_DIR="$WORK_DIR/aligned_data"
        REF_GENOME="$WORK_DIR/references/reference.fasta"
        
        # Input files
        R1_FILE="$WORK_DIR/raw_data/sample_{sample}_R1.fastq.gz"
        R2_FILE="$WORK_DIR/raw_data/sample_{sample}_R2.fastq.gz"
        
        # Output files
        SAM_FILE="$ALIGN_DIR/sample_{sample}.sam"
        BAM_FILE="$ALIGN_DIR/sample_{sample}.bam"
        SORTED_BAM="$ALIGN_DIR/sample_{sample}_sorted.bam"
        
        echo "Aligning reads for sample {sample}..."
        echo "Reference: $REF_GENOME"
        echo "Input R1: $R1_FILE"
        echo "Input R2: $R2_FILE"
        
        # Simulate BWA alignment
        echo "Running BWA mem alignment..."
        
        # Create mock SAM header
        {{
            echo "@HD	VN:1.6	SO:unsorted"
            echo "@SQ	SN:chr1	LN:1100"
            echo "@SQ	SN:chr2	LN:1100"
            echo "@PG	ID:bwa	PN:bwa	VN:0.7.17"
        }} > "$SAM_FILE"
        
        # Simulate aligned reads
        READ_COUNT=$(zcat "$R1_FILE" | wc -l | awk '{{print $1/4}}')
        echo "Processing $READ_COUNT read pairs..."
        
        for i in $(seq 1 $READ_COUNT); do
            # Simulate alignment (create mock SAM records)
            if [ $((i % 100)) -eq 0 ]; then
                echo "Aligned $i reads..."
            fi
            
            # Add a few mock aligned reads to SAM file
            if [ $((i % 100)) -eq 0 ]; then
                MAPQ=$((20 + RANDOM % 40))
                POS=$((RANDOM % 1000))
                echo "read_$i	99	chr1	$POS	$MAPQ	100M	=	$((POS + 200))	300	$(head -c 100 /dev/urandom | tr -dc 'ATCG')	$(head -c 100 /dev/urandom | tr -dc 'IIII')" >> "$SAM_FILE"
            fi
        done
        
        echo "Converting SAM to BAM..."
        # Simulate samtools view (just copy for demo)
        cp "$SAM_FILE" "$BAM_FILE.sam"
        echo "Mock BAM file created" > "$BAM_FILE"
        
        echo "Sorting BAM file..."
        cp "$BAM_FILE" "$SORTED_BAM"
        
        # Generate alignment statistics
        STATS_FILE="$ALIGN_DIR/sample_{sample}_stats.txt"
        {{
            echo "Alignment Statistics for Sample {sample}"
            echo "======================================"
            echo "Total reads: $READ_COUNT"
            echo "Aligned reads: $((READ_COUNT * 95 / 100))"
            echo "Alignment rate: 95%"
            echo "Average mapping quality: 35"
            echo "Properly paired: $((READ_COUNT * 90 / 100))"
            echo "Generated: $(date)"
        }} > "$STATS_FILE"
        
        echo "Alignment completed for sample {sample}"
        echo "Output files:"
        ls -la "$ALIGN_DIR/sample_{sample}"*
        """,
        executor_config={
            {
                "slurm": {
                    {
                        "partition": "normal",
                        "cpus_per_task": 8,  # BWA benefits from multiple cores
                        "mem": "16G",  # Memory-intensive for large genomes
                        "time_limit": "02:00:00",  # Alignment can take time
                    }
                }
            }
        },
        dag=dag,
    )
    alignment_tasks.append(align_task)

# Variant calling - CPU intensive
variant_calling = BashOperator(
    task_id="variant_calling",
    bash_command="""
    echo "=== Variant Calling ==="
    
    WORK_DIR="/tmp/bioinformatics_{{ ds_nodash }}"
    VARIANT_DIR="$WORK_DIR/variants"
    ALIGN_DIR="$WORK_DIR/aligned_data"
    REF_GENOME="$WORK_DIR/references/reference.fasta"
    
    # Output files
    VCF_FILE="$VARIANT_DIR/combined_variants.vcf"
    FILTERED_VCF="$VARIANT_DIR/filtered_variants.vcf"
    
    echo "Calling variants across all samples..."
    
    # List input BAM files
    BAM_FILES=""
    for sample in 001 002; do
        BAM_FILE="$ALIGN_DIR/sample_${sample}_sorted.bam"
        if [ -f "$BAM_FILE" ]; then
            BAM_FILES="$BAM_FILES $BAM_FILE"
            echo "Including: $BAM_FILE"
        fi
    done
    
    # Simulate variant calling with GATK/bcftools
    echo "Running variant calling..."
    
    # Create VCF header
    {
        echo "##fileformat=VCFv4.2"
        echo "##source=MockVariantCaller"
        echo "##reference=$REF_GENOME"
        echo "##contig=<ID=chr1,length=1100>"
        echo "##contig=<ID=chr2,length=1100>"
        echo '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">'
        echo '##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">'
        echo '##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">'
        echo -e "#CHROM\\tPOS\\tID\\tREF\\tALT\\tQUAL\\tFILTER\\tINFO\\tFORMAT\\tsample_001\\tsample_002"
    } > "$VCF_FILE"
    
    # Generate mock variants
    echo "Generating variants..."
    for chr in chr1 chr2; do
        for i in $(seq 1 50); do
            POS=$((100 + i * 20))
            REF=$(echo "ATCG" | cut -c $((RANDOM % 4 + 1)))
            ALT=$(echo "ATCG" | cut -c $((RANDOM % 4 + 1)))
            QUAL=$((20 + RANDOM % 80))
            DP1=$((10 + RANDOM % 50))
            DP2=$((10 + RANDOM % 50))
            GQ1=$((20 + RANDOM % 80))
            GQ2=$((20 + RANDOM % 80))
            
            if [ "$REF" != "$ALT" ]; then
                GT1="0/1"
                GT2="1/1"
                echo -e "$chr\\t$POS\\t.\\t$REF\\t$ALT\\t$QUAL\\tPASS\\t.\\tGT:DP:GQ\\t$GT1:$DP1:$GQ1\\t$GT2:$DP2:$GQ2" >> "$VCF_FILE"
            fi
        done
    done
    
    # Simulate variant filtering
    echo "Filtering variants..."
    
    # Copy header and filter variants
    grep "^#" "$VCF_FILE" > "$FILTERED_VCF"
    grep -v "^#" "$VCF_FILE" | awk '$6 >= 30' >> "$FILTERED_VCF"
    
    # Generate variant statistics
    STATS_FILE="$VARIANT_DIR/variant_stats.txt"
    {
        echo "Variant Calling Statistics"
        echo "========================="
        echo "Date: {{ ds }}"
        echo "Total variants called: $(grep -v "^#" "$VCF_FILE" | wc -l)"
        echo "High-quality variants: $(grep -v "^#" "$FILTERED_VCF" | wc -l)"
        echo "Variants per chromosome:"
        grep -v "^#" "$FILTERED_VCF" | cut -f1 | sort | uniq -c
        echo "Quality distribution:"
        echo "  Q30+: $(grep -v "^#" "$FILTERED_VCF" | awk '$6 >= 30' | wc -l)"
        echo "  Q50+: $(grep -v "^#" "$FILTERED_VCF" | awk '$6 >= 50' | wc -l)"
        echo "Generated: $(date)"
    } > "$STATS_FILE"
    
    echo "Variant calling completed"
    echo "Results:"
    echo "  Raw variants: $VCF_FILE"
    echo "  Filtered variants: $FILTERED_VCF"
    echo "  Statistics: $STATS_FILE"
    """,
    executor_config={
        "slurm": {
            "partition": "normal",
            "cpus_per_task": 4,
            "mem": "8G",
            "time_limit": "01:00:00",
        }
    },
    dag=dag,
)

# Annotation - database intensive
variant_annotation = BashOperator(
    task_id="variant_annotation",
    bash_command="""
    echo "=== Variant Annotation ==="
    
    WORK_DIR="/tmp/bioinformatics_{{ ds_nodash }}"
    VARIANT_DIR="$WORK_DIR/variants"
    FILTERED_VCF="$VARIANT_DIR/filtered_variants.vcf"
    ANNOTATED_VCF="$VARIANT_DIR/annotated_variants.vcf"
    
    echo "Annotating variants with functional consequences..."
    
    # Simulate variant annotation (VEP/SnpEff)
    if [ ! -f "$FILTERED_VCF" ]; then
        echo "Error: Filtered VCF not found: $FILTERED_VCF"
        exit 1
    fi
    
    # Copy header and add annotation fields
    grep "^##" "$FILTERED_VCF" > "$ANNOTATED_VCF"
    echo '##INFO=<ID=GENE,Number=1,Type=String,Description="Gene Symbol">' >> "$ANNOTATED_VCF"
    echo '##INFO=<ID=EFFECT,Number=1,Type=String,Description="Predicted Effect">' >> "$ANNOTATED_VCF"
    echo '##INFO=<ID=AF,Number=1,Type=Float,Description="Allele Frequency">' >> "$ANNOTATED_VCF"
    grep "^#CHROM" "$FILTERED_VCF" >> "$ANNOTATED_VCF"
    
    # Annotate variants
    GENE_NAMES=("BRCA1" "TP53" "EGFR" "KRAS" "PIK3CA" "MYC" "APC" "PTEN")
    EFFECTS=("missense" "synonymous" "nonsense" "splice_site" "intronic")
    
    grep -v "^#" "$FILTERED_VCF" | while IFS=$'\\t' read -r chrom pos id ref alt qual filter info format sample1 sample2; do
        # Assign random gene and effect
        gene=${GENE_NAMES[$((RANDOM % ${#GENE_NAMES[@]}))]}
        effect=${EFFECTS[$((RANDOM % ${#EFFECTS[@]}))]}
        af=$(echo "scale=4; $RANDOM / 32767" | bc)
        
        # Add annotations to INFO field
        new_info="GENE=$gene;EFFECT=$effect;AF=$af"
        if [ "$info" != "." ]; then
            new_info="$info;$new_info"
        fi
        
        echo -e "$chrom\\t$pos\\t$id\\t$ref\\t$alt\\t$qual\\t$filter\\t$new_info\\t$format\\t$sample1\\t$sample2" >> "$ANNOTATED_VCF"
    done
    
    # Generate annotation summary
    ANNOTATION_SUMMARY="$VARIANT_DIR/annotation_summary.txt"
    {
        echo "Variant Annotation Summary"
        echo "========================="
        echo "Date: {{ ds }}"
        echo "Annotated variants: $(grep -v "^#" "$ANNOTATED_VCF" | wc -l)"
        echo ""
        echo "Effect distribution:"
        grep -v "^#" "$ANNOTATED_VCF" | grep -o "EFFECT=[^;]*" | cut -d= -f2 | sort | uniq -c
        echo ""
        echo "Gene distribution (top 5):"
        grep -v "^#" "$ANNOTATED_VCF" | grep -o "GENE=[^;]*" | cut -d= -f2 | sort | uniq -c | sort -nr | head -5
        echo ""
        echo "Generated: $(date)"
    } > "$ANNOTATION_SUMMARY"
    
    echo "Variant annotation completed"
    echo "Output: $ANNOTATED_VCF"
    echo "Summary: $ANNOTATION_SUMMARY"
    """,
    executor_config={
        "slurm": {
            "partition": "normal",
            "cpus_per_task": 2,
            "mem": "4G",
            "time_limit": "00:30:00",
        }
    },
    dag=dag,
)

# Generate final report
generate_report = BashOperator(
    task_id="generate_final_report",
    bash_command="""
    echo "=== Generating Final Analysis Report ==="
    
    WORK_DIR="/tmp/bioinformatics_{{ ds_nodash }}"
    REPORT_DIR="$WORK_DIR/reports"
    FINAL_REPORT="$REPORT_DIR/analysis_report_{{ ds_nodash }}.html"
    
    mkdir -p "$REPORT_DIR"
    
    # Generate comprehensive HTML report
    cat > "$FINAL_REPORT" << 'EOF'
<!DOCTYPE html>
<html>
<head>
    <title>Bioinformatics Analysis Report - {{ ds }}</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        h1, h2 { color: #2c3e50; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
        th { background-color: #f2f2f2; }
        .summary { background-color: #e8f5e8; padding: 20px; border-radius: 5px; }
        .warning { background-color: #fff3cd; padding: 10px; border-radius: 3px; }
    </style>
</head>
<body>
    <h1>Bioinformatics Analysis Report</h1>
    <div class="summary">
        <h2>Analysis Summary</h2>
        <p><strong>Date:</strong> {{ ds }}</p>
        <p><strong>Pipeline Version:</strong> 1.0</p>
        <p><strong>Samples Processed:</strong> 2 (sample_001, sample_002)</p>
    </div>
    
    <h2>Quality Control Results</h2>
    <table>
        <tr><th>Sample</th><th>Total Reads</th><th>Avg Quality</th><th>Status</th></tr>
EOF
    
    # Add QC results to report
    for sample in 001 002; do
        QC_DIR="$WORK_DIR/quality_control/sample_$sample"
        if [ -d "$QC_DIR" ]; then
            READS=$(grep "Total Reads" "$QC_DIR"/*_R1_*.txt 2>/dev/null | head -1 | awk '{print $3}' || echo "N/A")
            QUALITY=$(grep "Average Quality" "$QC_DIR"/*_R1_*.txt 2>/dev/null | head -1 | awk '{print $3}' || echo "N/A")
            echo "        <tr><td>sample_$sample</td><td>$READS</td><td>$QUALITY</td><td>PASS</td></tr>" >> "$FINAL_REPORT"
        fi
    done
    
    cat >> "$FINAL_REPORT" << 'EOF'
    </table>
    
    <h2>Alignment Statistics</h2>
    <table>
        <tr><th>Sample</th><th>Alignment Rate</th><th>Properly Paired</th><th>Avg MAPQ</th></tr>
EOF
    
    # Add alignment results
    for sample in 001 002; do
        ALIGN_STATS="$WORK_DIR/aligned_data/sample_${sample}_stats.txt"
        if [ -f "$ALIGN_STATS" ]; then
            ALIGN_RATE=$(grep "Alignment rate" "$ALIGN_STATS" | awk '{print $3}' || echo "N/A")
            PAIRED=$(grep "Properly paired" "$ALIGN_STATS" | awk '{print $3}' || echo "N/A")
            MAPQ=$(grep "Average mapping quality" "$ALIGN_STATS" | awk '{print $4}' || echo "N/A")
            echo "        <tr><td>sample_$sample</td><td>$ALIGN_RATE</td><td>$PAIRED</td><td>$MAPQ</td></tr>" >> "$FINAL_REPORT"
        fi
    done
    
    cat >> "$FINAL_REPORT" << 'EOF'
    </table>
    
    <h2>Variant Calling Results</h2>
EOF
    
    # Add variant statistics
    VARIANT_STATS="$WORK_DIR/variants/variant_stats.txt"
    if [ -f "$VARIANT_STATS" ]; then
        TOTAL_VARIANTS=$(grep "Total variants called" "$VARIANT_STATS" | awk '{print $4}')
        HQ_VARIANTS=$(grep "High-quality variants" "$VARIANT_STATS" | awk '{print $3}')
        
        cat >> "$FINAL_REPORT" << EOF
    <p><strong>Total Variants Called:</strong> $TOTAL_VARIANTS</p>
    <p><strong>High-Quality Variants:</strong> $HQ_VARIANTS</p>
EOF
    fi
    
    # Add annotation summary
    ANNOTATION_SUMMARY="$WORK_DIR/variants/annotation_summary.txt"
    if [ -f "$ANNOTATION_SUMMARY" ]; then
        cat >> "$FINAL_REPORT" << 'EOF'
    
    <h2>Variant Annotation</h2>
    <p>Variants have been annotated with gene symbols and predicted functional effects.</p>
EOF
        
        ANNOTATED_COUNT=$(grep "Annotated variants" "$ANNOTATION_SUMMARY" | awk '{print $3}')
        echo "    <p><strong>Annotated Variants:</strong> $ANNOTATED_COUNT</p>" >> "$FINAL_REPORT"
    fi
    
    cat >> "$FINAL_REPORT" << EOF
    
    <h2>Files Generated</h2>
    <ul>
        <li>Quality Control Reports: $WORK_DIR/quality_control/</li>
        <li>Aligned Reads: $WORK_DIR/aligned_data/</li>
        <li>Variants (VCF): $WORK_DIR/variants/annotated_variants.vcf</li>
        <li>Analysis Report: $FINAL_REPORT</li>
    </ul>
    
    <div class="warning">
        <strong>Note:</strong> This is a demonstration pipeline with simulated data.
        In a real analysis, you would use actual bioinformatics tools like FastQC, BWA, GATK, and VEP.
    </div>
    
    <hr>
    <p><small>Generated on $(date) by Airflow Slurm Executor Pipeline</small></p>
</body>
</html>
EOF
    
    echo "Final report generated: $FINAL_REPORT"
    echo "Report size: $(wc -c < "$FINAL_REPORT") bytes"
    
    # Generate text summary for logs
    echo ""
    echo "=== ANALYSIS COMPLETE ==="
    echo "Samples processed: 2"
    echo "Variants identified: $(grep -v "^#" "$WORK_DIR/variants/annotated_variants.vcf" 2>/dev/null | wc -l || echo "0")"
    echo "Report: $FINAL_REPORT"
    echo "Workspace: $WORK_DIR"
    """,
    executor_config={
        "slurm": {
            "partition": "normal",
            "cpus_per_task": 1,
            "mem": "512M",
            "time_limit": "00:10:00",
        }
    },
    dag=dag,
)

# Archive results
archive_results = BashOperator(
    task_id="archive_results",
    bash_command="""
    echo "=== Archiving Results ==="
    
    WORK_DIR="/tmp/bioinformatics_{{ ds_nodash }}"
    ARCHIVE_DIR="/tmp/bioinfo_archives"
    mkdir -p "$ARCHIVE_DIR"
    
    # Create archive
    ARCHIVE_FILE="$ARCHIVE_DIR/analysis_{{ ds_nodash }}.tar.gz"
    
    echo "Creating archive: $ARCHIVE_FILE"
    tar -czf "$ARCHIVE_FILE" -C "/tmp" "bioinformatics_{{ ds_nodash }}"
    
    # Generate archive manifest
    MANIFEST_FILE="$ARCHIVE_DIR/manifest_{{ ds_nodash }}.txt"
    {
        echo "Bioinformatics Analysis Archive Manifest"
        echo "======================================="
        echo "Date: {{ ds }}"
        echo "Archive: $ARCHIVE_FILE"
        echo "Size: $(du -h "$ARCHIVE_FILE" | cut -f1)"
        echo ""
        echo "Contents:"
        tar -tzf "$ARCHIVE_FILE" | head -20
        echo "..."
        echo ""
        echo "Total files: $(tar -tzf "$ARCHIVE_FILE" | wc -l)"
    } > "$MANIFEST_FILE"
    
    echo "Archive created successfully"
    echo "Archive size: $(du -h "$ARCHIVE_FILE")"
    echo "Manifest: $MANIFEST_FILE"
    """,
    dag=dag,
)

# Cleanup
cleanup = BashOperator(
    task_id="cleanup",
    bash_command="""
    echo "=== Cleanup ==="
    
    WORK_DIR="/tmp/bioinformatics_{{ ds_nodash }}"
    
    if [ -d "$WORK_DIR" ]; then
        echo "Workspace size before cleanup:"
        du -sh "$WORK_DIR"
        
        # Remove large intermediate files but keep results
        find "$WORK_DIR" -name "*.sam" -delete
        find "$WORK_DIR" -name "*_sorted.bam" -delete
        find "$WORK_DIR/raw_data" -name "*.fastq.gz" -delete
        
        echo "Workspace size after cleanup:"
        du -sh "$WORK_DIR"
        
        echo "Cleanup completed"
    else
        echo "No workspace to clean"
    fi
    """,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

# End
end = DummyOperator(
    task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, dag=dag
)

# Define dependencies
start >> check_files

# Main processing path
check_files >> setup_workspace
setup_workspace >> qc_tasks
qc_tasks >> alignment_tasks
(
    alignment_tasks
    >> variant_calling
    >> variant_annotation
    >> generate_report
    >> archive_results
)

# Skip path
check_files >> no_files

# Both paths converge at cleanup
[archive_results, no_files] >> cleanup >> end
