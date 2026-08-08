package web

import "golang.org/x/net/html"

func findPushCountDiv(node *html.Node) *html.Node {
	return findDivWithClasses(node, "nrec")
}

func findArticleBlocks(node *html.Node) *html.Node {
	return findDivWithClasses(node, "r-ent")
}

func findPagingBlock(node *html.Node) *html.Node {
	return findDivWithClasses(node, "btn-group", "btn-group-paging")
}

func findMainBoardContainer(node *html.Node) *html.Node { return findDivByID(node, "main-container") }

func findBoardActionContainer(node *html.Node) *html.Node {
	return findDivByID(node, "action-bar-container")
}

func findBoardListContainer(node *html.Node) *html.Node {
	return findDivWithClasses(node, "r-list-container")
}
