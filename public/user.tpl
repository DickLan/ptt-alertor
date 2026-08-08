總人數：{{.Total}}
Discord: {{.Discord}}
BlockUser: {{.BlockUser}}, IdleUser: {{.IdleUser}}
User: {{.User}}, Room: {{.Room}}, Group: {{.Group}}
count(Board): {{.BoardCount}}, count(Keyword): {{.KeywordCount}}, count(Author): {{.AuthorCount}}, count(PushSum): {{.PushSumCount}}
{{range .Users}}
{{.Profile.Account}}{{end}}
