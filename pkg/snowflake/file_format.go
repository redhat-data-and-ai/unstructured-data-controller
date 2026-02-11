package snowflake

type UploadedFileStatus struct {
	Source string `db:"source"`
	Target string `db:"target"`
	Status string `db:"status"`
}
