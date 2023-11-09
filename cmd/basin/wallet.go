package main

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/urfave/cli/v2"
)

func newWalletCommand() *cli.Command {
	return &cli.Command{
		Name:  "wallet",
		Usage: "wallet commands",
		Subcommands: []*cli.Command{
			{
				Name:  "create",
				Usage: "creates a new wallet",
				Action: func(cCtx *cli.Context) error {
					filename := cCtx.Args().Get(0)
					if filename == "" {
						return errors.New("filename is empty")
					}

					privateKey, err := crypto.GenerateKey()
					if err != nil {
						return fmt.Errorf("generate key: %s", err)
					}
					privateKeyBytes := crypto.FromECDSA(privateKey)

					if err := os.WriteFile(filename, []byte(hexutil.Encode(privateKeyBytes)[2:]), 0o644); err != nil {
						return fmt.Errorf("writing to file %s: %s", filename, err)
					}
					pubk, _ := privateKey.Public().(*ecdsa.PublicKey)
					publicKey := common.HexToAddress(crypto.PubkeyToAddress(*pubk).Hex())

					fmt.Printf("Wallet address %s created\n", publicKey)
					fmt.Printf("Private key saved in %s\n", filename)
					return nil
				},
			},
			{
				Name:  "pubkey",
				Usage: "print the public key for a private key",
				Action: func(cCtx *cli.Context) error {
					filename := cCtx.Args().Get(0)
					if filename == "" {
						return errors.New("filename is empty")
					}

					privateKey, err := crypto.LoadECDSA(filename)
					if err != nil {
						return fmt.Errorf("loading key: %s", err)
					}

					pubk, _ := privateKey.Public().(*ecdsa.PublicKey)
					publicKey := common.HexToAddress(crypto.PubkeyToAddress(*pubk).Hex())

					fmt.Println(publicKey)
					return nil
				},
			},
		},
	}
}
