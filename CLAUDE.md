# Unstructure Data Controller
Untructured Data Controller is a Kubernetes Controller that processes raw documents (like pdfs, spreadsheets, documents, markdown files etc) into consumable by LLM powered MCPs and Agents. It implements various document processing and chunking strategies via Docling, Google Gemini API etc.

@README.md

## 📚 Documentation Structure

This CLAUDE.md uses modular organization with imports for better token efficiency:

**Core Documentation:**
- ARCHITECTURE.md - Architecture overview and diagrams

# Project Guidelines
- This is a Kubernetes Controller.
- Follow good software engineering patterns, specifically Go language patterns and Kuberntes controller best practices.
- Implement exactly what the idea describes. Do not improve, refactor, or update any code, documentation, or specification that is not directly required by the stated feature. If you discover something that should be improved but is outside scope, note it in a comment but do not act on it. A smaller, focused MR is always preferred over a comprehensive one.
- Always write tests where needed.

**Interface Specifications** (Sacred Contracts):
See `specs/` directory for complete interface documentation:



## References
- [Kubebuilder Webhook Local Testing](https://book.kubebuilder.io/reference/webhook-for-local-development.html)
- [kind Networking](https://kind.sigs.k8s.io/docs/user/ingress/#using-ingress)
- [host.docker.internal](https://docs.docker.com/desktop/networking/#i-want-to-connect-from-a-container-to-a-service-on-the-host)
