"""
Pagination utilities for the E-Commerce Analytics Platform API.

This module provides pagination support for API endpoints with consistent
response formatting and performance optimization.
"""

from math import ceil
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar

from fastapi import Query
from pydantic import BaseModel, Field, validator

T = TypeVar("T")


class PaginationParams(BaseModel):
    """Pagination parameters for API requests."""

    page: int = Field(default=1, ge=1, description="Page number (starts from 1)")
    size: int = Field(
        default=100, ge=1, le=1000, description="Number of items per page (max 1000)"
    )

    @validator("page")
    def validate_page(cls, v):
        """Validate page number."""
        if v < 1:
            raise ValueError("Page number must be at least 1")
        return v

    @validator("size")
    def validate_size(cls, v):
        """Validate page size."""
        if v < 1:
            raise ValueError("Page size must be at least 1")
        if v > 1000:
            raise ValueError("Page size cannot exceed 1000")
        return v

    @property
    def offset(self) -> int:
        """Calculate offset for database queries."""
        return (self.page - 1) * self.size

    @property
    def limit(self) -> int:
        """Get limit for database queries."""
        return self.size


class PaginationMeta(BaseModel):
    """Pagination metadata."""

    page: int = Field(description="Current page number")
    size: int = Field(description="Items per page")
    total_items: int = Field(description="Total number of items")
    total_pages: int = Field(description="Total number of pages")
    has_next: bool = Field(description="Whether there is a next page")
    has_previous: bool = Field(description="Whether there is a previous page")
    next_page: Optional[int] = Field(description="Next page number")
    previous_page: Optional[int] = Field(description="Previous page number")


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response model."""

    items: List[T] = Field(description="List of items for current page")
    pagination: PaginationMeta = Field(description="Pagination metadata")

    class Config:
        """Pydantic configuration."""

        arbitrary_types_allowed = True


def create_pagination_meta(page: int, size: int, total_items: int) -> PaginationMeta:
    """
    Create pagination metadata.

    Args:
        page: Current page number
        size: Items per page
        total_items: Total number of items

    Returns:
        PaginationMeta object
    """
    total_pages = ceil(total_items / size) if total_items > 0 else 0
    has_next = page < total_pages
    has_previous = page > 1

    return PaginationMeta(
        page=page,
        size=size,
        total_items=total_items,
        total_pages=total_pages,
        has_next=has_next,
        has_previous=has_previous,
        next_page=page + 1 if has_next else None,
        previous_page=page - 1 if has_previous else None,
    )


def paginate_list(items: List[T], pagination: PaginationParams) -> PaginatedResponse[T]:
    """
    Paginate a list of items.

    Args:
        items: List of items to paginate
        pagination: Pagination parameters

    Returns:
        Paginated response
    """
    total_items = len(items)
    start_idx = pagination.offset
    end_idx = start_idx + pagination.size

    page_items = items[start_idx:end_idx]
    meta = create_pagination_meta(pagination.page, pagination.size, total_items)

    return PaginatedResponse(items=page_items, pagination=meta)


def paginate_query_result(
    items: List[T], total_count: int, pagination: PaginationParams
) -> PaginatedResponse[T]:
    """
    Create paginated response from query results.

    Args:
        items: Items for current page
        total_count: Total number of items across all pages
        pagination: Pagination parameters

    Returns:
        Paginated response
    """
    meta = create_pagination_meta(pagination.page, pagination.size, total_count)

    return PaginatedResponse(items=items, pagination=meta)


def get_pagination_params(
    page: int = Query(1, ge=1, description="Page number (starts from 1)"),
    size: int = Query(
        100, ge=1, le=1000, description="Number of items per page (max 1000)"
    ),
) -> PaginationParams:
    """
    FastAPI dependency for pagination parameters.

    Args:
        page: Page number
        size: Page size

    Returns:
        PaginationParams object
    """
    return PaginationParams(page=page, size=size)


class CursorPagination(BaseModel):
    """Cursor-based pagination for large datasets."""

    cursor: Optional[str] = Field(default=None, description="Cursor for next page")
    size: int = Field(
        default=100, ge=1, le=1000, description="Number of items per page"
    )


class CursorPaginationMeta(BaseModel):
    """Cursor pagination metadata."""

    size: int = Field(description="Items per page")
    has_next: bool = Field(description="Whether there is a next page")
    has_previous: bool = Field(description="Whether there is a previous page")
    next_cursor: Optional[str] = Field(description="Cursor for next page")
    previous_cursor: Optional[str] = Field(description="Cursor for previous page")


class CursorPaginatedResponse(BaseModel, Generic[T]):
    """Cursor-based paginated response."""

    items: List[T] = Field(description="List of items for current page")
    pagination: CursorPaginationMeta = Field(description="Cursor pagination metadata")

    class Config:
        """Pydantic configuration."""

        arbitrary_types_allowed = True


def create_cursor_pagination_meta(
    size: int,
    has_next: bool,
    has_previous: bool = False,
    next_cursor: Optional[str] = None,
    previous_cursor: Optional[str] = None,
) -> CursorPaginationMeta:
    """
    Create cursor pagination metadata.

    Args:
        size: Items per page
        has_next: Whether there is a next page
        has_previous: Whether there is a previous page
        next_cursor: Cursor for next page
        previous_cursor: Cursor for previous page

    Returns:
        CursorPaginationMeta object
    """
    return CursorPaginationMeta(
        size=size,
        has_next=has_next,
        has_previous=has_previous,
        next_cursor=next_cursor,
        previous_cursor=previous_cursor,
    )


# Pagination response helpers
def create_paginated_response(
    items: List[Dict[str, Any]],
    total_count: int,
    pagination: PaginationParams,
    additional_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Create a standardized paginated response.

    Args:
        items: Items for current page
        total_count: Total number of items
        pagination: Pagination parameters
        additional_data: Additional data to include in response

    Returns:
        Standardized paginated response dictionary
    """
    meta = create_pagination_meta(pagination.page, pagination.size, total_count)

    response = {
        "items": items,
        "pagination": meta.dict(),
        "summary": {
            "total_items": total_count,
            "current_page_items": len(items),
            "page": pagination.page,
            "total_pages": meta.total_pages,
        },
    }

    if additional_data:
        response.update(additional_data)

    return response


def optimize_pagination_for_large_datasets(
    total_count: int, requested_page: int, page_size: int, max_efficient_page: int = 100
) -> Tuple[bool, Optional[str]]:
    """
    Check if pagination request is efficient for large datasets.

    Args:
        total_count: Total number of items
        requested_page: Requested page number
        page_size: Items per page
        max_efficient_page: Maximum efficient page number

    Returns:
        Tuple of (is_efficient, suggestion_message)
    """
    if requested_page > max_efficient_page:
        suggestion = (
            f"For performance reasons, consider using cursor-based pagination "
            f"or filtering to reduce the dataset size. "
            f"Requesting page {requested_page} of {ceil(total_count / page_size)} "
            f"may result in slow response times."
        )
        return False, suggestion

    return True, None


class PaginationOptimizer:
    """Utility class for pagination optimization strategies."""

    @staticmethod
    def should_use_cursor_pagination(
        total_count: int,
        requested_page: int,
        threshold_count: int = 10000,
        threshold_page: int = 50,
    ) -> bool:
        """
        Determine if cursor pagination should be recommended.

        Args:
            total_count: Total number of items
            requested_page: Requested page number
            threshold_count: Count threshold for cursor pagination
            threshold_page: Page threshold for cursor pagination

        Returns:
            True if cursor pagination is recommended
        """
        return total_count > threshold_count or requested_page > threshold_page

    @staticmethod
    def get_cache_key_for_pagination(
        base_key: str,
        pagination: PaginationParams,
        filters: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Generate cache key for paginated results.

        Args:
            base_key: Base cache key
            pagination: Pagination parameters
            filters: Optional filters applied

        Returns:
            Cache key for paginated results
        """
        key_parts = [base_key, f"page_{pagination.page}", f"size_{pagination.size}"]

        if filters:
            # Sort filters for consistent key generation
            sorted_filters = sorted(filters.items())
            filter_str = "_".join(
                [f"{k}_{v}" for k, v in sorted_filters if v is not None]
            )
            if filter_str:
                key_parts.append(filter_str)

        return ":".join(key_parts)

    @staticmethod
    def calculate_optimal_page_size(
        total_count: int,
        target_response_time_ms: int = 200,
        base_processing_time_per_item_ms: float = 1.0,
    ) -> int:
        """
        Calculate optimal page size based on performance targets.

        Args:
            total_count: Total number of items
            target_response_time_ms: Target response time in milliseconds
            base_processing_time_per_item_ms: Base processing time per item

        Returns:
            Optimal page size
        """
        max_items_for_target = int(
            target_response_time_ms / base_processing_time_per_item_ms
        )

        # Cap at reasonable limits
        optimal_size = min(max_items_for_target, 1000)
        optimal_size = max(optimal_size, 10)  # Minimum of 10 items

        return optimal_size
