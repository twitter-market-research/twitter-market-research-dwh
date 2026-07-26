# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

# ── Chemin vers le code source
sys.path.insert(0, os.path.abspath("../.."))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'twitter-research'
copyright = '2026, Sémakia'
author = 'Sémakia'
release = '0.1'
language = 'en'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration
# ── Extensions ───────────────────────────────────────────────────────────────
extensions = [
    "sphinx.ext.autodoc",           # génère la doc depuis les docstrings
    "sphinx.ext.napoleon",          # supporte Google-style et NumPy-style docstrings
    "sphinx_autodoc_typehints",     # affiche les types Python dans la doc
    "sphinx.ext.viewcode",          # ajoute un lien "voir le source" dans la doc
    "sphinx.ext.autosummary",       # génère des tableaux résumés automatiquement
]

html_theme = "sphinx_rtd_theme"
html_theme_options = {
    "navigation_depth": 4,
    "titles_only": False,
}

autodoc_default_options = {
    "members": True,            # documenter toutes les méthodes publiques
    "undoc-members": True,      # inclure même sans docstring
    "private-members": False,   # exclure les méthodes _privées
    "show-inheritance": True,   # afficher l'héritage de classe
}
autodoc_typehints = "description"   # affiche les types dans la description
napoleon_google_docstring = True
napoleon_numpy_docstring = False

templates_path = ["_templates"]
exclude_patterns = []
