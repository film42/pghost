package replication

import (
	"context"
	"errors"
	"time"

	"github.com/film42/pghost/config"
	"github.com/film42/pghost/pglogrepl"
	"github.com/jackc/pgx/v4"
)

func WaitForHotStandbyToArriveAtLSN(ctx context.Context, cfg *config.Config, lsn string) error {
	srcConn, err := pgx.Connect(ctx, cfg.SourceConnection)
	if err != nil {
		return err
	}
	defer srcConn.Close(ctx)

	consistencyPointLsn, err := pglogrepl.ParseLSN(lsn)
	if err != nil {
		return err
	}

	var currentRcvLsn string
	for {
		err := srcConn.QueryRow(ctx, "select pg_last_wal_receive_lsn()").Scan(&currentRcvLsn)
		if err != nil {
			return err
		}

		if len(currentRcvLsn) == 0 {
			return errors.New("error: Could not get LSN using pg_last_wal_receive_lsn(): probably not a hot standby server")
		}

		hotStandbyLsn, err := pglogrepl.ParseLSN(currentRcvLsn)
		if err != nil {
			return err
		}

		if hotStandbyLsn >= consistencyPointLsn {
			return nil
		}

		time.Sleep(time.Second)
	}
}
