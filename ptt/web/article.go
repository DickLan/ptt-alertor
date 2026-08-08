package web

import "golang.org/x/net/html"

func findTitleDiv(node *html.Node) *html.Node {
	return findDivWithClasses(node, "title")
}

func findMetaDiv(node *html.Node) *html.Node {
	return findDivWithClasses(node, "meta")
}

func findDateDiv(node *html.Node) *html.Node {
	return findDivWithClasses(node, "date")
}

func findAuthorDiv(node *html.Node) *html.Node {
	return findDivWithClasses(node, "author")
}

func findDividerDiv(node *html.Node) *html.Node {
	return findDivWithClasses(node, "r-list-sep")
}

func findOgTitleMeta(node *html.Node) *html.Node {
	return findMeta(node, "og:title")
}

func findMainArticleContent(node *html.Node) *html.Node {
	if node.Type != html.ElementNode || node.Data != "div" {
		return nil
	}
	for _, attr := range node.Attr {
		if attr.Key == "id" && attr.Val == "main-content" {
			return node
		}
	}
	return nil
}

func findEmailProtected(node *html.Node) *html.Node {
	n := findAnchor(node)
	if n != nil {
		for _, attr := range n.Attr {
			if attr.Key == "class" && attr.Val == "__cf_email__" {
				return n
			}
		}
	}
	return nil
}
