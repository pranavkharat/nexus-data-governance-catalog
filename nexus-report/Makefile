# ============================================================
# NEXUS Report - Makefile for Pandoc PDF Generation
# ============================================================
# Usage:
#   make pdf      - Generate PDF report
#   make word     - Generate Word document
#   make html     - Generate HTML document
#   make all      - Generate all formats
#   make clean    - Remove generated files
# ============================================================

# Source file
SOURCE = NEXUS_Report.md

# Output files
PDF = NEXUS_Report.pdf
DOCX = NEXUS_Report.docx
HTML = NEXUS_Report.html
TEX = NEXUS_Report.tex

# Pandoc options
PANDOC = pandoc
PDF_ENGINE = --pdf-engine=xelatex
TOC = --toc --toc-depth=3
SECTIONS = --number-sections
HIGHLIGHT = --highlight-style=tango

# Geometry and formatting
GEOMETRY = -V geometry:margin=1in
FONT = -V fontsize=11pt
SPACING = -V linestretch=1.5
DOCCLASS = -V documentclass=report
COLORS = -V colorlinks=true -V linkcolor=blue -V urlcolor=blue

# Bibliography (uncomment if using citations)
# BIB = --bibliography=references.bib --citeproc

# ============================================================
# TARGETS
# ============================================================

.PHONY: all pdf word html tex clean help

# Default target
all: pdf word

# Generate PDF (main target)
pdf: $(PDF)

$(PDF): $(SOURCE)
	@echo "📄 Generating PDF..."
	$(PANDOC) $(SOURCE) -o $(PDF) \
		$(TOC) \
		$(SECTIONS) \
		$(PDF_ENGINE) \
		$(GEOMETRY) \
		$(FONT) \
		$(SPACING) \
		$(DOCCLASS) \
		$(COLORS) \
		$(HIGHLIGHT)
	@echo "✅ PDF generated: $(PDF)"

# Generate Word document
word: $(DOCX)

$(DOCX): $(SOURCE)
	@echo "📝 Generating Word document..."
	$(PANDOC) $(SOURCE) -o $(DOCX) $(TOC)
	@echo "✅ Word document generated: $(DOCX)"

# Generate HTML
html: $(HTML)

$(HTML): $(SOURCE)
	@echo "🌐 Generating HTML..."
	$(PANDOC) $(SOURCE) -o $(HTML) \
		$(TOC) \
		$(SECTIONS) \
		--standalone \
		--self-contained \
		$(HIGHLIGHT)
	@echo "✅ HTML generated: $(HTML)"

# Generate LaTeX source
tex: $(TEX)

$(TEX): $(SOURCE)
	@echo "📐 Generating LaTeX source..."
	$(PANDOC) $(SOURCE) -o $(TEX) \
		$(TOC) \
		$(SECTIONS)
	@echo "✅ LaTeX generated: $(TEX)"

# Clean generated files
clean:
	@echo "🧹 Cleaning generated files..."
	rm -f $(PDF) $(DOCX) $(HTML) $(TEX)
	@echo "✅ Clean complete"

# Help
help:
	@echo "NEXUS Report - Pandoc Build System"
	@echo ""
	@echo "Usage:"
	@echo "  make pdf    - Generate PDF report (recommended)"
	@echo "  make word   - Generate Word document"
	@echo "  make html   - Generate HTML document"
	@echo "  make tex    - Generate LaTeX source"
	@echo "  make all    - Generate PDF and Word"
	@echo "  make clean  - Remove generated files"
	@echo ""
	@echo "Prerequisites:"
	@echo "  - Pandoc installed"
	@echo "  - LaTeX distribution (MacTeX/MiKTeX/TeXLive)"
	@echo "  - Images in ./images/ folder"