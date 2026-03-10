from __future__ import annotations
from typing import Any
import httpx
import polars as pl
from plugins.base import SourcePlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class APIReader(SourcePlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="api_reader",
            name="Read API",
            category=PluginCategory.SOURCE,
            description="Read data from a REST API endpoint (JSON response)",
            icon="Globe",
            color="#22c55e",
            inputs=[],
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="url", label="URL", param_type="string", required=True, description="API endpoint URL"),
                ParamDefinition(name="method", label="HTTP Method", param_type="select", default="GET", options=["GET", "POST", "PUT", "PATCH", "DELETE"], required=False, description="HTTP method"),
                ParamDefinition(name="headers", label="Headers (JSON)", param_type="code", default="{}", required=False, description='e.g. {"Authorization": "Bearer ..."}'),
                ParamDefinition(name="query_params", label="Query Parameters (JSON)", param_type="code", default="{}", required=False, description='e.g. {"page": 1, "limit": 100}'),
                ParamDefinition(name="body", label="Request Body (JSON)", param_type="code", default="", required=False, description="JSON body for POST/PUT/PATCH"),
                ParamDefinition(name="json_path", label="JSON Path (dot notation)", param_type="string", default="", required=False, description="Extract nested data, e.g. data.items"),
                ParamDefinition(name="timeout", label="Timeout (seconds)", param_type="number", default=60, required=False, description="Request timeout"),
                ParamDefinition(name="auth_type", label="Auth Type", param_type="select", default="none", options=["none", "basic", "bearer"], required=False, description="Authentication method"),
                ParamDefinition(name="auth_user", label="Auth Username / Bearer Token", param_type="string", default="", required=False, description="Username for Basic auth, or Bearer token"),
                ParamDefinition(name="auth_password", label="Auth Password", param_type="string", default="", required=False, description="Password for Basic auth"),
                ParamDefinition(name="pagination_type", label="Pagination", param_type="select", default="none", options=["none", "offset", "cursor"], required=False, description="Paginate through multiple requests"),
                ParamDefinition(name="page_param", label="Page/Offset Param Name", param_type="string", default="page", required=False, description="Query param for page number or offset"),
                ParamDefinition(name="page_size_param", label="Page Size Param Name", param_type="string", default="limit", required=False, description="Query param for page size"),
                ParamDefinition(name="page_size", label="Page Size", param_type="number", default=100, required=False, description="Number of records per page"),
                ParamDefinition(name="max_pages", label="Max Pages", param_type="number", default=10, required=False, description="Maximum pages to fetch (safety limit)"),
                ParamDefinition(name="cursor_path", label="Cursor Path", param_type="string", default="", required=False, description="JSON path to next-page cursor for cursor pagination"),
                ParamDefinition(name="verify_ssl", label="Verify SSL", param_type="boolean", default=True, required=False, description="Verify SSL certificates"),
                ParamDefinition(name="follow_redirects", label="Follow Redirects", param_type="boolean", default=True, required=False, description="Automatically follow HTTP redirects"),
            ],
        )

    def _build_client_kwargs(self) -> dict[str, Any]:
        p = self.params
        kwargs: dict[str, Any] = dict(
            timeout=float(p.get("timeout", 60)),
            verify=p.get("verify_ssl", True),
            follow_redirects=p.get("follow_redirects", True),
        )
        auth_type = p.get("auth_type", "none")
        if auth_type == "basic":
            kwargs["auth"] = (p.get("auth_user", ""), p.get("auth_password", ""))
        return kwargs

    def _build_request_kwargs(self) -> dict[str, Any]:
        import json as _json
        p = self.params
        kwargs: dict[str, Any] = dict(
            method=p.get("method", "GET"),
            url=p["url"],
        )

        headers = _json.loads(p.get("headers", "{}") or "{}")
        auth_type = p.get("auth_type", "none")
        if auth_type == "bearer":
            headers["Authorization"] = f"Bearer {p.get('auth_user', '')}"
        if headers:
            kwargs["headers"] = headers

        qp = _json.loads(p.get("query_params", "{}") or "{}")
        if qp:
            kwargs["params"] = qp

        body = p.get("body", "")
        if body:
            kwargs["json"] = _json.loads(body)
        return kwargs

    @staticmethod
    def _extract(data: Any, path: str) -> Any:
        if path:
            for key in path.split("."):
                if isinstance(data, list):
                    data = [item[key] for item in data]
                else:
                    data = data[key]
        return data

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        p = self.params
        client_kw = self._build_client_kwargs()
        req_kw = self._build_request_kwargs()
        json_path = p.get("json_path", "")
        pagination = p.get("pagination_type", "none")

        all_records: list[dict] = []

        with httpx.Client(**client_kw) as client:
            if pagination == "none":
                resp = client.request(**req_kw)
                resp.raise_for_status()
                data = self._extract(resp.json(), json_path)
                all_records = data if isinstance(data, list) else [data]
            elif pagination == "offset":
                page_param = p.get("page_param", "page")
                ps_param = p.get("page_size_param", "limit")
                page_size = int(p.get("page_size", 100))
                max_pages = int(p.get("max_pages", 10))
                params = req_kw.pop("params", {})
                for page in range(max_pages):
                    params[page_param] = page
                    params[ps_param] = page_size
                    resp = client.request(**req_kw, params=params)
                    resp.raise_for_status()
                    data = self._extract(resp.json(), json_path)
                    batch = data if isinstance(data, list) else [data]
                    if not batch:
                        break
                    all_records.extend(batch)
                    if len(batch) < page_size:
                        break
            elif pagination == "cursor":
                cursor_path = p.get("cursor_path", "")
                max_pages = int(p.get("max_pages", 10))
                for _ in range(max_pages):
                    resp = client.request(**req_kw)
                    resp.raise_for_status()
                    body = resp.json()
                    data = self._extract(body, json_path)
                    batch = data if isinstance(data, list) else [data]
                    if not batch:
                        break
                    all_records.extend(batch)
                    cursor = self._extract(body, cursor_path) if cursor_path else None
                    if not cursor:
                        break
                    params = req_kw.get("params", {})
                    params["cursor"] = cursor
                    req_kw["params"] = params

        df = pl.DataFrame(all_records) if all_records else pl.DataFrame()
        return {"output": df.lazy()}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        url = params["url"]
        method = params.get("method", "GET")
        timeout = float(params.get("timeout", 60))
        json_path = params.get("json_path", "")
        var = "__OUT__"
        lines = [
            "import httpx",
            f"_resp = httpx.request({method!r}, {url!r}, timeout={timeout})",
            "_resp.raise_for_status()",
            "_data = _resp.json()",
        ]
        if json_path:
            for key in json_path.split("."):
                lines.append(f"_data = _data[{key!r}]")
        lines.append(f"{var} = pl.DataFrame(_data if isinstance(_data, list) else [_data]).lazy()")
        return "\n".join(lines), {"output": var}
