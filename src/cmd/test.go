/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"github.com/spf13/cobra"
)

// testCmd represents the test command
var testCmd = &cobra.Command{
	Use:   "test",
	Short: "Run local/remote tests using subcommands and supported flags",
	Long: `Run local/remote tests using subcommands and supported flags`,
	//Run: func(cmd *cobra.Command, args []string) {
	//	fmt.Println("test called")
	//},
}

func init() {
	rootCmd.AddCommand(testCmd)

	testCmd.PersistentFlags().String("name","", "lowercase name of test to run")
	testCmd.PersistentFlags().Bool("correctness", false, "Run correctness test")
	testCmd.PersistentFlags().Bool("perf", false, "Run performance test")
	testCmd.PersistentFlags().StringP("output", "o", "../output/output.txt", "Define output file")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// testCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// testCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
