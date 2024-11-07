package models

type Task struct {
	ID   string `json:"task_id"`
	Type string `json:"task"`
	Args []int  `json:"args"`
}
