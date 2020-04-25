#!/usr/bin/env bash
#
# Build the whole report, including the references.

pdflatex report.tex
bibtex report
pdflatex report.tex
pdflatex report.tex
