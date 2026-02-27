"""Public generate() function for fyrnheim code generation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from fyrnheim.core import Entity
from fyrnheim.core.source_mapping import SourceMapping
from fyrnheim.generators import IbisCodeGenerator


@dataclass(frozen=True)
class GenerateResult:
    """Result of generating transformation code for one entity."""

    entity_name: str
    code: str
    output_path: Path
    written: bool


def generate(
    entity: Entity,
    output_dir: str | Path = "generated",
    dry_run: bool = False,
    source_mapping: SourceMapping | None = None,
) -> GenerateResult:
    """Generate Ibis transformation code for a single entity.

    Args:
        entity: Pydantic Entity instance defining the business object.
        output_dir: Directory to write the generated module into.
            Created if it does not exist.
        dry_run: If True, generate code but do not write to disk.

    Returns:
        GenerateResult with the generated code and output file path.
    """
    output_dir = Path(output_dir)
    output_path = output_dir / f"{entity.name}_transforms.py"

    generator = IbisCodeGenerator(entity, source_mapping=source_mapping)
    code = generator.generate_module()

    if dry_run:
        return GenerateResult(
            entity_name=entity.name,
            code=code,
            output_path=output_path,
            written=False,
        )

    # Create output dir
    output_dir.mkdir(parents=True, exist_ok=True)

    # Skip write if content unchanged
    if output_path.exists() and output_path.read_text() == code:
        return GenerateResult(
            entity_name=entity.name,
            code=code,
            output_path=output_path,
            written=False,
        )

    output_path.write_text(code)
    return GenerateResult(
        entity_name=entity.name,
        code=code,
        output_path=output_path,
        written=True,
    )
