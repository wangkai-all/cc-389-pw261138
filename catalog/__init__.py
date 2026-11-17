"""Tenant-isolated catalog migration system.

Shared tables -> per-tenant isolated storage, with public dimensions
kept as a single read-only shared, versioned store.
"""
