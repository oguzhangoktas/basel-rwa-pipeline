# Architecture Diagrams

## Creating Diagrams

Use [https://app.diagrams.net/](https://app.diagrams.net/) (formerly draw.io) to create professional architecture diagrams.

## Recommended Diagrams

### 1. `architecture.png` - High-Level System Architecture

**Components to include:**
- Source systems (Oracle, External APIs)
- AWS services (S3, Glue, Redshift, Athena)
- Orchestration (Airflow)
- Monitoring (CloudWatch)
- Consumption layer (Qlik Sense)

**Template**: Use AWS Architecture diagram template in draw.io

### 2. `data_flow.png` - Detailed Data Flow

**Show:**
- Raw → Staging → Processed layers
- Each transformation stage
- Data volumes at each step
- Processing times

### 3. `airflow_dag.png` - Workflow Visualization

**Include:**
- All Airflow tasks
- Dependencies between tasks
- SLA indicators
- Retry logic

## Tips

- Use consistent colors for each layer (blue for S3, orange for Glue, etc.)
- Add annotations for key metrics (processing time, data volume)
- Keep it readable - don't overcrowd
- Export as PNG with transparent background
- Recommend size: 1920x1080 pixels

## Quick Start

1. Go to https://app.diagrams.net/
2. Choose "AWS Architecture" template
3. Drag and drop AWS service icons
4. Add arrows to show data flow
5. Export → PNG
6. Save to this folder

## Example Flow Diagram Structure

```
┌─────────┐
│ Source  │
└────┬────┘
     │
     ↓
┌─────────┐
│   S3    │
└────┬────┘
     │
     ↓
┌─────────┐
│  Spark  │
└────┬────┘
     │
     ↓
┌─────────┐
│Redshift │
└─────────┘
```
