# Implementation Notes: Default Jobs Taxonomy for dbt Model Classification

## Summary

This implementation adds a default jobs taxonomy for dbt model classification in Fyrnheim. The taxonomy provides an opinionated set of classification rules that identify what job each dbt model performs, focusing on functional purpose rather than organizational layer.

## Key Components

1. **Default Taxonomy Rules** (`src/fyrnheim/default_jobs_taxonomy.yml`): A YAML file containing 7 distinct job classifications for dbt models:
   - Source Mapping
   - Format Alignment
   - Data Contract
   - Entity Definition
   - Business Rules
   - Analytical Output
   - Quality Validation

2. **Taxonomy Loader** (`src/fyrnheim/dbt_default_taxonomy.py`): Python module that loads and validates the default taxonomy, providing utility functions for working with it.

3. **CLI Integration** (`src/fyrnheim/cli.py`): Modified `fyr dbt classify` command to support a `--use-default` flag that applies the default taxonomy without requiring a custom rules file.

4. **Documentation** (`docs/dbt-default-jobs-taxonomy.md`): Comprehensive documentation explaining the taxonomy, each job type, and usage examples.

5. **Tests** (`tests/test_dbt_default_taxonomy.py`): Complete test suite validating all functionality.

## Design Decisions

### Jobs vs. Layers Approach

Unlike traditional layer-based approaches (staging/intermediate/marts), the jobs taxonomy focuses on what each model actually does rather than where it lives in the codebase. This approach:

- Makes model purpose more explicit
- Enables clearer discussions about data transformation work
- Provides a more precise framework for AI-assisted changes
- Complements rather than replaces layer concepts

### Seven Job Categories

The taxonomy includes seven distinct jobs based on the blog post and mission requirements:

1. **Source Mapping**: Maps raw source data into a standardized format
2. **Format Alignment**: Standardizes data formats, types, and naming conventions
3. **Data Contract**: Enforces data quality standards and schema compliance
4. **Entity Definition**: Defines core business entities and their attributes
5. **Business Rules**: Applies domain-specific business logic and calculations
6. **Analytical Output**: Shapes data for specific analytical use cases and reports
7. **Quality Validation**: Validates data quality and monitors for anomalies

Each job includes:
- Clear description of what it does
- Typical signals for identification
- Examples and anti-examples
- Suggested principles
- Relationship to common layer labels

### Implementation Choices

1. **Default `allow_multiple_labels: true`**: Models can perform multiple jobs, reflecting real-world complexity.

2. **CLI Integration**: Added `--use-default` flag to `fyr dbt classify` for easy application of the taxonomy.

3. **Validation**: Built-in validation ensures the taxonomy remains consistent and complete.

4. **Extensibility**: The taxonomy can be exported and customized while providing a solid starting point.

## Tradeoffs

### Benefits
- Provides immediate value without requiring custom configuration
- Makes model purpose explicit through job-based classification
- Enables inspectable and adjustable rules for better AI collaboration
- Works with existing M115 inventory artifacts
- Includes comprehensive documentation and examples

### Limitations
- May not capture all domain-specific job types
- Requires understanding of job concepts vs. layer concepts
- Initial setup requires learning the taxonomy
- May need customization for specific organizational needs

## Risks & Mitigations

### Risk 1: Taxonomy may be too opinionated
**Mitigation**: Taxonomy is designed to be exportable and customizable. Users can modify rules or create entirely new taxonomies.

### Risk 2: Model may perform jobs not covered by taxonomy
**Mitigation**: The classification engine supports extending the taxonomy with custom rules. Models that don't match any rules are marked as unclassified.

### Risk 3: Signal overlap between jobs
**Mitigation**: Priority system ensures consistent classification when multiple rules could apply. Users can adjust priorities in custom taxonomies.

## Usage Examples

### CLI Classification with Default Taxonomy:
```bash
fyr dbt classify --inventory inventory.json --use-default
```

### Programmatic Usage:
```python
from fyrnheim.dbt_default_taxonomy import load_default_taxonomy
from fyrnheim.dbt_classification import classify_inventory

taxonomy = load_default_taxonomy()
classification = classify_inventory(inventory, taxonomy)
```

### Customization:
```bash
# Export default taxonomy
fyr dbt classify --use-default --format json > custom-taxonomy.yml

# Modify and use custom taxonomy
fyr dbt classify --inventory inventory.json --rules custom-taxonomy.yml
```

## Future Improvements

1. **Empirical Validation**: Test taxonomy across more diverse dbt projects
2. **Additional Job Types**: Expand based on community feedback
3. **Integration with Other Tools**: Connect classification to automated refactoring suggestions
4. **Dynamic Signal Detection**: Improve automatic identification of job signals