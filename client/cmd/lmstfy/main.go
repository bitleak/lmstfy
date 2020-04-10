package main

import (
	"fmt"
	"os"
	"time"

	"github.com/bitleak/lmstfy/client"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile      string
	lmstfyClient *client.LmstfyClient
)

func initLmstfyClient() {
	if cfgFile == "" {
		viper.SetConfigName(".lmstfy")
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println("Failed to get home directory")
			os.Exit(1)
		}
		viper.AddConfigPath(home)
	} else {
		viper.SetConfigFile(cfgFile)
	}
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Printf("Failed to load config: %s", err)
		os.Exit(1)
	}
	host := viper.GetString("host")
	port := viper.GetInt("port")
	namespace := viper.GetString("namespace")
	token := viper.GetString("token")
	lmstfyClient = client.NewLmstfyClient(host, port, namespace, token)
}

func main() {
	cobra.OnInitialize(initLmstfyClient)

	publishCmd := &cobra.Command{
		Use:     "publish [queue] [job data]",
		Short:   "publish a job to queue",
		Example: `publish test "hello world"`,
		Aliases: []string{"put", "pub"},
		Args:    cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			ttl, _ := cmd.Flags().GetUint32("ttl")
			tries, _ := cmd.Flags().GetUint16("tries")
			delay, _ := cmd.Flags().GetUint32("delay")
			jobID, err := lmstfyClient.Publish(args[0], []byte(args[1]), ttl, tries, delay)
			if err != nil {
				fmt.Printf("Failed: %s\n", err)
			} else {
				fmt.Printf("Job ID: %s\n", jobID)
			}
		},
	}
	publishCmd.Flags().Uint32P("ttl", "t", 0, "time-to-live in second, no TTL by default")
	publishCmd.Flags().Uint16P("tries", "r", 1, "number of tries")
	publishCmd.Flags().Uint32P("delay", "d", 0, "delay in second, no delay by default")

	consumeCmd := &cobra.Command{
		Use:     "consume [queue]",
		Short:   "consume a job from queue",
		Example: "consume test",
		Aliases: []string{"get", "con"},
		Args:    cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ttr, err := cmd.Flags().GetUint32("ttr")
			timeout, _ := cmd.Flags().GetUint32("timeout")
			job, err := lmstfyClient.Consume(args[0], ttr, timeout)
			if err != nil {
				fmt.Printf("Failed: %s\n", err)
			} else if job == nil {
				fmt.Println("No job available")
			} else {
				fmt.Printf("Job ID: %s\n", job.ID)
				fmt.Printf("Job data: %s\n", string(job.Data))
				fmt.Printf("* TTL: %s\n", time.Duration(job.TTL)*time.Second)
				fmt.Printf("* Elapsed: %s\n", time.Duration(job.ElapsedMS)*time.Millisecond)
			}
		},
	}
	consumeCmd.Flags().Uint32P("ttr", "t", 120, "time-to-run in second")
	consumeCmd.Flags().Uint32P("timeout", "w", 10, "blocking wait timeout in second")

	ackCmd := &cobra.Command{
		Use:     "ack [queue] [job ID]",
		Short:   "acknowledge the job, mark it as finished",
		Example: "ack test 01CG14G3JKF840QHZB6NR1NHVJ",
		Aliases: []string{"del"},
		Args:    cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			err := lmstfyClient.Ack(args[0], args[1])
			if err != nil {
				fmt.Printf("Failed: %s\n", err)
			} else {
				fmt.Println("ACK")
			}
		},
	}

	sizeCmd := &cobra.Command{
		Use:     "size [queue]",
		Short:   "get the queue size, and related deadletter size",
		Example: "size test",
		Aliases: []string{"len"},
		Args:    cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			qSize, err := lmstfyClient.QueueSize(args[0])
			if err != nil {
				fmt.Printf("Failed: %s\n", err)
				return
			}
			dSize, dHead, err := lmstfyClient.PeekDeadLetter(args[0])
			if err != nil {
				fmt.Printf("Failed: %s\n", err)
				return
			}
			fmt.Printf("Queue size: %d\n", qSize)
			fmt.Printf("DeadLetter size: %d\n", dSize)
			if dSize > 0 {
				fmt.Printf("DeadLetter head: %s\n", dHead)
			}
		},
	}

	peekCmd := &cobra.Command{
		Use:     "peek [queue] [job ID]",
		Short:   "peek a job without consuming it",
		Example: "peek test\npeek test 01CG14G3JKF840QHZB6NR1NHVJ",
		Args:    cobra.RangeArgs(1, 2),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 1 {
				job, err := lmstfyClient.PeekQueue(args[0])
				if err != nil {
					fmt.Printf("Failed: %s\n", err)
					return
				}
				if job == nil {
					fmt.Printf("Not found\n")
					return
				}
				fmt.Printf("Job ID: %s\n", job.ID)
				fmt.Printf("Job data: %s\n", string(job.Data))
				fmt.Printf("* TTL: %s\n", time.Duration(job.TTL)*time.Second)
				fmt.Printf("* Elapsed: %s\n", time.Duration(job.ElapsedMS)*time.Millisecond)
			} else {
				job, err := lmstfyClient.PeekJob(args[0], args[1])
				if err != nil {
					fmt.Printf("Failed: %s\n", err)
					return
				}
				if job == nil {
					fmt.Printf("Not found\n")
					return
				}
				fmt.Printf("Job data: %s\n", string(job.Data))
				fmt.Printf("* TTL: %s\n", time.Duration(job.TTL)*time.Second)
				fmt.Printf("* Elapsed: %s\n", time.Duration(job.ElapsedMS)*time.Millisecond)
			}
		},
	}

	respawnCmd := &cobra.Command{
		Use:     "respawn [queue]",
		Short:   "respawn a job or all the jobs in the deadletter",
		Example: "respawn test",
		Aliases: []string{"kick"},
		Args:    cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			limit, _ := cmd.Flags().GetInt64("limit")
			ttl, _ := cmd.Flags().GetInt64("ttl")
			count, err := lmstfyClient.RespawnDeadLetter(args[0], limit, ttl)
			if err != nil {
				fmt.Printf("Failed: %s\n", err)
			} else {
				fmt.Printf("Respawn [%d] jobs", count)
			}
		},
	}
	respawnCmd.Flags().Int64P("limit", "l", 1, "upper limit of the number of dead jobs to be respawned")
	respawnCmd.Flags().Int64P("ttl", "t", 86400, "time-to-live in second, no TTL by default")

	rootCmd := &cobra.Command{Use: "lmstfy"}
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file path")

	rootCmd.AddCommand(publishCmd, consumeCmd, ackCmd, sizeCmd, peekCmd, respawnCmd)
	rootCmd.Execute()
}
