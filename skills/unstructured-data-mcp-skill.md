# Unstructured Data MCP Skill

You have access to an MCP server that lets you search documents and knowledge bases stored in Snowflake using vector similarity.

## Tools & Usage

1. **`list_unstructured_data_pipelines_for_user`** — Returns pipelines the user can access along with their descriptions. Call at the start. If the user asks about a pipeline not in the original list, call again to refresh.
2. **`get_chunks_for_embeddings`** — Semantic search over a pipeline's documents. Takes `pipeline_name` and `query`. Returns matching chunks with file IDs. Use descriptive natural language queries (not keywords — it's vector search). For broad questions, make multiple focused searches rather than one vague one.
3. **`get_processed_document`** — Fetches the full processed markdown of a document by file ID. Use this when chunks from `get_chunks_for_embeddings` don't give enough context to answer confidently.
4. **`get_citation_image`** — Returns the source page a chunk was taken from, as an image. Takes `pipeline_name` and the `chunk_id` of a chunk you already have, plus `mode`, which you should always set explicitly to either `"highlight"` (the full page with the cited regions marked, for showing where an answer came from) or `"crop"` (just those regions cut out, when only the cited text matters). Pick one and call the tool once per chunk; calling it again with the other mode returns the same citation in a different shape, not new evidence. Use it to show the reader where an answer came from. Only PDFs and images have pages to render, so a chunk from a spreadsheet or a web page says so rather than failing.

## Key Rules

- Only use pipeline names returned by `list_unstructured_data_pipelines_for_user`. Never guess.
- Ground every answer in the returned content. If the data doesn't contain the answer, say so.
- If a tool call fails, tell the user there was a technical issue. Don't interpret errors as content.
- One pipeline per search call. If spanning multiple pipelines, make separate calls and attribute answers clearly.
